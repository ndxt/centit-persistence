package com.centit.framework.flyway.plugin;

import com.centit.framework.flyway.plugin.kingbase.KingBaseDbSupport;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.dbsupport.DbSupport;
import org.flywaydb.core.internal.dbsupport.FlywaySqlException;
import org.flywaydb.core.internal.dbsupport.oracle.OracleDbSupport;
import org.flywaydb.core.internal.util.logging.Log;
import org.flywaydb.core.internal.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Factory for obtaining the correct DbSupport instance for the current connection.
 */
public class DbSupportFactoryExt {
    private static final Log LOG = LogFactory.getLog(DbSupportFactoryExt.class);

    /**
     * Prevent instantiation.
     */
    private DbSupportFactoryExt() {
        //Do nothing
    }

    /**
     * Initializes the appropriate DbSupport class for the database product used by the data source.
     *
     * @param connection The Jdbc connection to use to query the database.
     * @param printInfo  Where the DB info should be printed in the logs.
     * @return The appropriate DbSupport class.
     */
    public static DbSupport createDbSupport(Connection connection, boolean printInfo) {
        String databaseProductName = getDatabaseProductName(connection);

        if (printInfo) {
            LOG.info("Database: " + getJdbcUrl(connection) + " (" + databaseProductName + ")");
        }

        if (databaseProductName.startsWith("DM")) {
            return new OracleDbSupport(connection);
        }
        if (databaseProductName.startsWith("KingbaseES")||databaseProductName.startsWith("OSCAR")) {
            return new KingBaseDbSupport(connection);
        }

        //return DbSupportFactory.createDbSupport(connection, printInfo);
        throw new FlywayException("Unsupported Database: " + databaseProductName);
    }

    /**
     * Retrieves the Jdbc Url for this connection.
     *
     * @param connection The Jdbc connection.
     * @return The Jdbc Url.
     */

    private static String getJdbcUrl(Connection connection) {
        try {
            return connection.getMetaData().getURL();
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to retrieve the Jdbc connection Url!", e);
        }
    }

    /**
     * Retrieves the name of the database product.
     *
     * @param connection The connection to use to query the database.
     * @return The name of the database product. Ex.: Oracle, MySQL, ...
     */
    private static String getDatabaseProductName(Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData == null) {
                throw new FlywayException("Unable to read database metadata while it is null!");
            }

            String databaseProductName = databaseMetaData.getDatabaseProductName();
            if (databaseProductName == null) {
                throw new FlywayException("Unable to determine database. Product name is null.");
            }

            int databaseMajorVersion = databaseMetaData.getDatabaseMajorVersion();
            int databaseMinorVersion = databaseMetaData.getDatabaseMinorVersion();

            return databaseProductName + " " + databaseMajorVersion + "." + databaseMinorVersion;
        } catch (SQLException e) {
            throw new FlywaySqlException("Error while determining database product name", e);
        }
    }

    /**
     * Retrieves the database version.
     *
     * @param connection The connection to use to query the database.
     * @return The version of the database product.
     * Ex.: DSN11015 DB2 for z/OS Version 11
     * SQL10050 DB" for Linux, UNIX and Windows Version 10.5
     */
    private static String getDatabaseProductVersion(Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData == null) {
                throw new FlywayException("Unable to read database metadata while it is null!");
            }

            String databaseProductVersion = databaseMetaData.getDatabaseProductVersion();
            if (databaseProductVersion == null) {
                throw new FlywayException("Unable to determine database. Product version is null.");
            }


            return databaseProductVersion;
        } catch (SQLException e) {
            throw new FlywaySqlException("Error while determining database product version", e);
        }
    }

    /**
     * Retrieves the name of the JDBC driver
     *
     * @param connection The connection to use to query the database.
     * @return The name of the driver. Ex: RedshiftJDBC
     */
    private static String getDriverName(Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData == null) {
                throw new FlywayException("Unable to read database metadata while it is null!");
            }

            String driverName = databaseMetaData.getDriverName();
            if (driverName == null) {
                throw new FlywayException("Unable to determine JDBC  driver name. JDBC driver name is null.");
            }

            return driverName;
        } catch (SQLException e) {
            throw new FlywaySqlException("Error while determining JDBC driver name", e);
        }
    }
}

