package com.centit.framework.flyway.plugin;

import org.flywaydb.core.internal.dbsupport.oracle.OracleDbSupport;

import java.sql.Connection;
import java.sql.SQLException;

public class KBaseDbSupport extends OracleDbSupport {

    /**
     * Creates a new instance.
     *
     * @param connection The connection to use.
     */
    public KBaseDbSupport(Connection connection) {
        super(connection);
    }
    public String getDbName() {
        return "kbase";
    }
    @Override
    protected String doGetCurrentSchemaName() throws SQLException {
        return jdbcTemplate.queryForString("SELECT CURRENT_SCHEMA()");
    }
    @Override
    protected void doChangeCurrentSchemaTo(String schema) throws SQLException {
        jdbcTemplate.execute("SET search_path to " + quote(schema));
    }
}
