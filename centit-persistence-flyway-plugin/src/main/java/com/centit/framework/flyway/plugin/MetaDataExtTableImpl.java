package com.centit.framework.flyway.plugin;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.internal.dbsupport.*;
import org.flywaydb.core.internal.metadatatable.AppliedMigration;
import org.flywaydb.core.internal.metadatatable.MetaDataTableImpl;
import org.flywaydb.core.internal.util.PlaceholderReplacer;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;
import org.flywaydb.core.internal.util.scanner.classpath.ClassPathResource;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class MetaDataExtTableImpl extends MetaDataTableImpl {
    private final Table table;
    private static final Log LOG = LogFactory.getLog(MetaDataExtTableImpl.class);
    private final DbSupport dbSupport;
    private final JdbcTemplate jdbcTemplate;
    private String installedBy;
    public MetaDataExtTableImpl(DbSupport dbSupport, Table table, String installedBy){
        super(dbSupport,table,installedBy);
        this.table = table;
        this.dbSupport = dbSupport;
        jdbcTemplate = dbSupport.getJdbcTemplate();
        if (installedBy == null) {
            this.installedBy = dbSupport.getCurrentUserFunction();
        } else {
            this.installedBy = "'" + installedBy + "'";
        }
    }
    public void createIfNotExists() {
        int retries = 0;
        while (!table.exists()) {
            if (retries == 0) {
                LOG.info("Creating Metadata table: " + table);
            }

            try {
                String resourceName = "com/centit/framework/flyway/plugin/" + dbSupport.getDbName() + "/createMetaDataTable.sql";
                String source = new ClassPathResource(resourceName, getClass().getClassLoader()).loadAsString("UTF-8");

                Map<String, String> placeholders = new HashMap<String, String>();
                placeholders.put("schema", table.getSchema().getName());
                placeholders.put("table", table.getName());
                String sourceNoPlaceholders = new PlaceholderReplacer(placeholders, "${", "}").replacePlaceholders(source);

                final SqlScript sqlScript = new SqlScript(sourceNoPlaceholders, dbSupport);
                sqlScript.execute(jdbcTemplate);

                LOG.debug("Metadata table " + table + " created.");
            } catch (FlywayException e) {
                if (++retries >= 10) {
                    throw e;
                }
                try {
                    LOG.debug("Metadata table creation failed. Retrying in 1 sec ...");
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    // Ignore
                }
            }
        }
    }
    private int calculateInstalledRank() throws SQLException {
        int currentMax = jdbcTemplate.queryForInt("SELECT MAX(" + dbSupport.quote("installed_rank") + ")"
            + " FROM " + table);
        return currentMax + 1;
    }
    @Override
    public <T> T lock(Callable<T> callable) {
        createIfNotExists();
        return dbSupport.lock(table, callable);
    }

    @Override
    public void addAppliedMigration(AppliedMigration appliedMigration) {
        dbSupport.changeCurrentSchemaTo(table.getSchema());
        createIfNotExists();

        MigrationVersion version = appliedMigration.getVersion();

        try {
            String versionStr = version == null ? null : version.toString();

            // Try load an updateMetaDataTable.sql file if it exists
            String resourceName = "com/centit/framework/flyway/plugin/" + dbSupport.getDbName() + "/updateMetaDataTable.sql";
            ClassPathResource classPathResource = new ClassPathResource(resourceName, getClass().getClassLoader());
            int installedRank = calculateInstalledRank();
            if (classPathResource.exists()) {
                String source = classPathResource.loadAsString("UTF-8");
                Map<String, String> placeholders = new HashMap<String, String>();

                // Placeholders for schema and table
                placeholders.put("schema", table.getSchema().getName());
                placeholders.put("table", table.getName());

                // Placeholders for column values
                placeholders.put("installed_rank_val", String.valueOf(installedRank));
                placeholders.put("version_val", versionStr);
                placeholders.put("description_val", appliedMigration.getDescription());
                placeholders.put("type_val", appliedMigration.getType().name());
                placeholders.put("script_val", appliedMigration.getScript());
                placeholders.put("checksum_val", String.valueOf(appliedMigration.getChecksum()));
                placeholders.put("installed_by_val", installedBy);
                placeholders.put("execution_time_val", String.valueOf(appliedMigration.getExecutionTime() * 1000L));
                placeholders.put("success_val", String.valueOf(appliedMigration.isSuccess()));

                String sourceNoPlaceholders = new PlaceholderReplacer(placeholders, "${", "}").replacePlaceholders(source);

                SqlScript sqlScript = new SqlScript(sourceNoPlaceholders, dbSupport);

                sqlScript.execute(jdbcTemplate);
            } else {
                // Fall back to hard-coded statements
                jdbcTemplate.update("INSERT INTO " + table
                        + " (" + dbSupport.quote("installed_rank")
                        + "," + dbSupport.quote("version")
                        + "," + dbSupport.quote("description")
                        + "," + dbSupport.quote("type")
                        + "," + dbSupport.quote("script")
                        + "," + dbSupport.quote("checksum")
                        + "," + dbSupport.quote("installed_by")
                        + "," + dbSupport.quote("execution_time")
                        + "," + dbSupport.quote("success")
                        + ")"
                        + " VALUES (?, ?, ?, ?, ?, ?, " + installedBy + ", ?, ?)",
                    installedRank,
                    versionStr,
                    appliedMigration.getDescription(),
                    appliedMigration.getType().name(),
                    appliedMigration.getScript(),
                    appliedMigration.getChecksum(),
                    appliedMigration.getExecutionTime(),
                    appliedMigration.isSuccess()
                );
            }

            LOG.debug("MetaData table " + table + " successfully updated to reflect changes");
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to insert row for version '" + version + "' in metadata table " + table, e);
        }
    }
}
