package com.centit.framework.core.config;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationInfoService;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.flywaydb.core.api.configuration.FlywayConfiguration;
import org.flywaydb.core.api.resolver.MigrationResolver;
import org.flywaydb.core.internal.callback.SqlScriptFlywayCallback;
import org.flywaydb.core.internal.command.DbBaseline;
import org.flywaydb.core.internal.command.DbClean;
import org.flywaydb.core.internal.command.DbMigrate;
import org.flywaydb.core.internal.command.DbRepair;
import org.flywaydb.core.internal.command.DbSchemas;
import org.flywaydb.core.internal.command.DbValidate;
import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.DbSupportFactory;
import org.flywaydb.core.internal.dbsupport.Schema;
import org.flywaydb.core.internal.dbsupport.oracle.OracleDbSupport;
import org.flywaydb.core.internal.info.MigrationInfoServiceImpl;
import org.flywaydb.core.internal.metadatatable.MetaDataTable;
import org.flywaydb.core.internal.metadatatable.MetaDataTableImpl;
import org.flywaydb.core.internal.resolver.CompositeMigrationResolver;
import org.flywaydb.core.internal.util.ClassUtils;
import org.flywaydb.core.internal.util.ConfigurationInjectionUtils;
import org.flywaydb.core.internal.util.Locations;
import org.flywaydb.core.internal.util.PlaceholderReplacer;
import org.flywaydb.core.internal.util.StringUtils;
import org.flywaydb.core.internal.util.VersionPrinter;
import org.flywaydb.core.internal.util.jdbc.DriverDataSource;
import org.flywaydb.core.internal.util.jdbc.JdbcUtils;
import org.flywaydb.core.internal.util.jdbc.TransactionTemplate;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;
import org.flywaydb.core.internal.util.scanner.Scanner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * This is the centre point of Flyway, and for most users, the only class they will ever have to deal with.
 * <p>
 * It is THE public API from which all important Flyway functions such as clean, validate and migrate can be called.
 * </p>
 */
public class FlywayDM extends Flyway {
    private static final Log LOG = LogFactory.getLog(FlywayDM.class);


    /**
     * Creates a new instance of Flyway. This is your starting point.
     */
    public FlywayDM() {
        // Do nothing
    }

    private Locations locations = new Locations("db/migration");
    private boolean dbConnectionInfoPrinted;

    public void setLocations(String... locations) {
        this.locations = new Locations(locations);
    }

    /**
     * Executes this command with proper resource handling and cleanup.
     *
     * @param command The command to execute.
     * @param <T>     The type of the result.
     * @return The result of the command.
     */
    /*private -> testing*/ <T> T execute(Command<T> command) {
        T result;

        VersionPrinter.printVersion();

        Connection connectionMetaDataTable = null;

        try {
            if (getDataSource() == null) {
                throw new FlywayException("Unable to connect to the database. Configure the url, user and password!");
            }

            connectionMetaDataTable = JdbcUtils.openConnection(getDataSource());

            DbSupport dbSupport = new OracleDbSupport(connectionMetaDataTable);
            dbConnectionInfoPrinted = true;
            LOG.debug("DDL Transactions Supported: " + dbSupport.supportsDdlTransactions());

            if (getSchemas().length == 0) {
                Schema currentSchema = dbSupport.getOriginalSchema();
                if (currentSchema == null) {
                    throw new FlywayException("Unable to determine schema for the metadata table." +
                        " Set a default schema for the connection or specify one using the schemas property!");
                }
                setSchemas(currentSchema.getName());
            }

            if (getSchemas().length == 1) {
                LOG.debug("Schema: " + getSchemas()[0]);
            } else {
                LOG.debug("Schemas: " + StringUtils.arrayToCommaDelimitedString(getSchemas()));
            }

            Schema[] schemas = new Schema[getSchemas().length];
            for (int i = 0; i < getSchemas().length; i++) {
                schemas[i] = dbSupport.getSchema(getSchemas()[i]);
            }

            Scanner scanner = new Scanner(getClassLoader());
            MigrationResolver migrationResolver = createMigrationResolver(dbSupport, scanner);

            if (!isSkipDefaultCallbacks()) {
                Set<FlywayCallback> flywayCallbacks = new LinkedHashSet<FlywayCallback>(Arrays.asList(getCallbacks()));
                flywayCallbacks.add(
                    new SqlScriptFlywayCallback(dbSupport, scanner, locations, createPlaceholderReplacer(), this));
                setCallbacks(flywayCallbacks.toArray(new FlywayCallback[flywayCallbacks.size()]));
            }

            for (FlywayCallback callback : getCallbacks()) {
                ConfigurationInjectionUtils.injectFlywayConfiguration(callback, this);
            }

            MetaDataTable metaDataTable = new MetaDataTableImpl(dbSupport, schemas[0].getTable(getTable()), getInstalledBy());
            if (metaDataTable.upgradeIfNecessary()) {
                new DbRepair(dbSupport, connectionMetaDataTable, schemas[0], migrationResolver, metaDataTable, getCallbacks()).repairChecksumsAndDescriptions();
                LOG.info("Metadata table " + getTable() + " successfully upgraded to the Flyway 4.0 format.");
            }

            result = command.execute(connectionMetaDataTable, migrationResolver, metaDataTable, dbSupport, schemas, getCallbacks());
        } finally {
            JdbcUtils.closeConnection(connectionMetaDataTable);
        }
        return result;
    }

    private MigrationResolver createMigrationResolver(DbSupport dbSupport, Scanner scanner) {
        for (MigrationResolver resolver : getResolvers()) {
            ConfigurationInjectionUtils.injectFlywayConfiguration(resolver, this);
        }

        return new CompositeMigrationResolver(dbSupport, scanner, this, locations, createPlaceholderReplacer(), getResolvers());
    }

    private PlaceholderReplacer createPlaceholderReplacer() {
        if (isPlaceholderReplacement()) {
            return new PlaceholderReplacer(getPlaceholders(), getPlaceholderPrefix(), getPlaceholderSuffix());
        }
        return PlaceholderReplacer.NO_PLACEHOLDERS;
    }

    /**
     * A Flyway command that can be executed.
     *
     * @param <T> The result type of the command.
     */
    /*private -> testing*/ interface Command<T> {
        /**
         * Execute the operation.
         *
         * @param connectionMetaDataTable The database connection for the metadata table changes.
         * @param migrationResolver       The migration resolver to use.
         * @param metaDataTable           The metadata table.
         * @param dbSupport               The database-specific support for these connections.
         * @param schemas                 The schemas managed by Flyway.   @return The result of the operation.
         * @param flywayCallbacks         The callbacks to use.
         */
        T execute(Connection connectionMetaDataTable, MigrationResolver migrationResolver, MetaDataTable metaDataTable, DbSupport dbSupport, Schema[] schemas, FlywayCallback[] flywayCallbacks);
    }

    public int migrate() throws FlywayException {
        return execute(new Command<Integer>() {
            public Integer execute(Connection connectionMetaDataTable,
                                   MigrationResolver migrationResolver, MetaDataTable metaDataTable, DbSupport dbSupport, Schema[] schemas, FlywayCallback[] flywayCallbacks) {


                new DbSchemas(connectionMetaDataTable, schemas, metaDataTable).create();

                if (!metaDataTable.exists()) {
                    List<Schema> nonEmptySchemas = new ArrayList<Schema>();
                    for (Schema schema : schemas) {
                        if (!schema.empty()) {
                            nonEmptySchemas.add(schema);
                        }
                    }

                    if (!nonEmptySchemas.isEmpty()) {
                        if (isBaselineOnMigrate()) {
                            new DbBaseline(connectionMetaDataTable, dbSupport, metaDataTable, schemas[0], getBaselineVersion(), getBaselineDescription(), flywayCallbacks).baseline();
                        } else {
                            // Second check for MySQL which is sometimes flaky otherwise
                            if (!metaDataTable.exists()) {
                                throw new FlywayException("Found non-empty schema(s) "
                                    + StringUtils.collectionToCommaDelimitedString(nonEmptySchemas)
                                    + " without metadata table! Use baseline()"
                                    + " or set baselineOnMigrate to true to initialize the metadata table.");
                            }
                        }
                    }
                }

                Connection connectionUserObjects = null;
                try {
                    connectionUserObjects =
                        dbSupport.useSingleConnection() ? connectionMetaDataTable : JdbcUtils.openConnection(getDataSource());
                    DbMigrateDM dbMigrate =
                        new DbMigrateDM(connectionUserObjects, dbSupport, metaDataTable,
                            schemas[0], migrationResolver, isIgnoreFailedFutureMigration(), FlywayDM.this);
                    return dbMigrate.migrate();
                } finally {
                    if (!dbSupport.useSingleConnection()) {
                        JdbcUtils.closeConnection(connectionUserObjects);
                    }
                }
            }
        });
    }
}
