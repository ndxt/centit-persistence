package com.centit.support.database.metadata;

import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OracleMetadata implements DatabaseMetadata {

    private static final String SQL_TABLES =
        "SELECT TABLE_NAME, TABLE_TYPE, COMMENTS FROM all_tab_comments " +
            "WHERE OWNER=? AND TABLE_TYPE IN ('TABLE', 'VIEW')";

    private static final String SQL_COLUMNS =
        "SELECT a.COLUMN_NAME, a.DATA_TYPE, " +
            "CASE WHEN a.CHAR_USED='C' THEN a.CHAR_LENGTH ELSE a.DATA_LENGTH END AS DATA_LENGTH, " +
            "a.DATA_PRECISION, a.DATA_SCALE, a.NULLABLE, c.COMMENTS, %s AS IDENTITY_COLUMN, a.DATA_DEFAULT " +
            "FROM all_tab_columns a LEFT JOIN all_col_comments c " +
            "ON c.OWNER=a.OWNER AND c.TABLE_NAME=a.TABLE_NAME AND c.COLUMN_NAME=a.COLUMN_NAME " +
            "WHERE a.OWNER=? AND a.TABLE_NAME=? ORDER BY a.COLUMN_ID";

    private static final String SQL_PRIMARY_KEY =
        "SELECT c.CONSTRAINT_NAME, k.COLUMN_NAME, k.POSITION " +
            "FROM all_constraints c JOIN all_cons_columns k " +
            "ON k.OWNER=c.OWNER AND k.CONSTRAINT_NAME=c.CONSTRAINT_NAME " +
            "WHERE c.OWNER=? AND c.TABLE_NAME=? AND c.CONSTRAINT_TYPE='P' ORDER BY k.POSITION";

    private static final String SQL_REFERENCES =
        "SELECT fk.TABLE_NAME, fk.CONSTRAINT_NAME, p.COLUMN_NAME AS PARENT_COLUMN, " +
            "f.COLUMN_NAME AS CHILD_COLUMN " +
            "FROM all_constraints fk JOIN all_constraints pk " +
            "ON pk.OWNER=fk.R_OWNER AND pk.CONSTRAINT_NAME=fk.R_CONSTRAINT_NAME " +
            "JOIN all_cons_columns f ON f.OWNER=fk.OWNER AND f.CONSTRAINT_NAME=fk.CONSTRAINT_NAME " +
            "JOIN all_cons_columns p ON p.OWNER=pk.OWNER AND p.CONSTRAINT_NAME=pk.CONSTRAINT_NAME " +
            "AND p.POSITION=f.POSITION " +
            "WHERE pk.OWNER=? AND pk.TABLE_NAME=? AND fk.OWNER=? AND fk.CONSTRAINT_TYPE='R' " +
            "ORDER BY fk.CONSTRAINT_NAME, f.POSITION";

    private String sDBSchema;
    private Connection dbc;

    @Override
    public void setDBConfig(Connection dbc) {
        this.dbc = dbc;
    }

    @Override
    public String getDBSchema() {
        try {
            return resolveSchema();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to determine Oracle schema", e);
        }
    }

    @Override
    public void setDBSchema(String schema) {
        sDBSchema = schema;
    }

    private String resolveSchema() throws SQLException {
        if (StringUtils.isNotBlank(sDBSchema)) {
            return sDBSchema;
        }
        String schema;
        try {
            schema = dbc.getSchema();
        } catch (SQLFeatureNotSupportedException | AbstractMethodError e) {
            schema = null;
        }
        if (StringUtils.isBlank(schema)) {
            schema = dbc.getMetaData().getUserName();
        }
        if (StringUtils.isBlank(schema)) {
            throw new SQLException("Cannot determine Oracle schema; setDBSchema is required");
        }
        return schema;
    }

    @Override
    public List<SimpleTableInfo> listTables(boolean withColumn, String[] tableNames) {
        try {
            String schema = resolveSchema();
            List<SimpleTableInfo> tables = new ArrayList<>(100);
            try (PreparedStatement stmt = dbc.prepareStatement(SQL_TABLES)) {
                stmt.setString(1, schema);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString("TABLE_NAME");
                        if (tableNames != null && !StringUtils.equalsAnyIgnoreCase(name, tableNames)) {
                            continue;
                        }
                        SimpleTableInfo tab = readTableInfo(rs, schema);
                        if (withColumn) {
                            fetchTableDetail(tab);
                        }
                        tables.add(tab);
                    }
                }
            }
            return tables;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read Oracle table metadata", e);
        }
    }

    private SimpleTableInfo readTableInfo(ResultSet rs, String schema) throws SQLException {
        SimpleTableInfo tab = new SimpleTableInfo(rs.getString("TABLE_NAME"));
        tab.setSchema(schema);
        tab.setTableComment(rs.getString("COMMENTS"));
        tab.setTableLabelName(StringUtils.substring(tab.getTableComment(), 0, 80));
        tab.setTableType("VIEW".equalsIgnoreCase(rs.getString("TABLE_TYPE")) ? "V" : "T");
        return tab;
    }

    private void fetchTableDetail(SimpleTableInfo tab) throws SQLException {
        DatabaseMetaData metadata = dbc.getMetaData();
        // Older Oracle versions and other products sharing this class do not expose this catalog column.
        boolean supportsIdentity = "Oracle".equalsIgnoreCase(metadata.getDatabaseProductName())
            && metadata.getDatabaseMajorVersion() >= 12;
        String columnSql = String.format(SQL_COLUMNS, supportsIdentity ? "a.IDENTITY_COLUMN" : "'NO'");
        try (PreparedStatement stmt = dbc.prepareStatement(columnSql)) {
            stmt.setString(1, tab.getSchema());
            stmt.setString(2, tab.getTableName());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    SimpleTableField field = new SimpleTableField();
                    field.setColumnName(rs.getString("COLUMN_NAME"));
                    field.setColumnType(rs.getString("DATA_TYPE"));
                    int precision = rs.getInt("DATA_PRECISION");
                    field.setMaxLength(precision > 0 ? precision : rs.getInt("DATA_LENGTH"));
                    field.setScale(rs.getInt("DATA_SCALE"));
                    field.setNullEnable(rs.getString("NULLABLE"));
                    field.setColumnComment(rs.getString("COMMENTS"));
                    field.setFieldLabelName(StringUtils.substring(field.getColumnComment(), 0, 80));
                    field.mapToMetadata();
                    boolean identity = "YES".equalsIgnoreCase(rs.getString("IDENTITY_COLUMN"));
                    // DATA_DEFAULT is LONG in the Oracle catalog; keep it last and read it last.
                    String defaultValue = StringUtils.trimToNull(rs.getString("DATA_DEFAULT"));
                    if (identity) {
                        field.setFieldType(FieldType.IDENTITY);
                    } else {
                        field.setDefaultValue(defaultValue);
                    }
                    tab.addColumn(field);
                }
            }
        }
        try (PreparedStatement stmt = dbc.prepareStatement(SQL_PRIMARY_KEY)) {
            stmt.setString(1, tab.getSchema());
            stmt.setString(2, tab.getTableName());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tab.setPkName(rs.getString("CONSTRAINT_NAME"));
                    SimpleTableField field = tab.findFieldByColumn(rs.getString("COLUMN_NAME"));
                    if (field == null) {
                        throw new SQLException("Primary key column is absent from " + tab.getTableName());
                    }
                    field.setPrimaryKey(true);
                    field.setPrimaryKeyOrder(rs.getInt("POSITION"));
                }
            }
        }
        Map<String, SimpleTableReference> references = new LinkedHashMap<>();
        try (PreparedStatement stmt = dbc.prepareStatement(SQL_REFERENCES)) {
            stmt.setString(1, tab.getSchema());
            stmt.setString(2, tab.getTableName());
            stmt.setString(3, tab.getSchema());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("CONSTRAINT_NAME");
                    SimpleTableReference ref = references.get(name);
                    if (ref == null) {
                        ref = new SimpleTableReference();
                        ref.setParentTableName(tab.getTableName());
                        ref.setTableName(rs.getString("TABLE_NAME"));
                        ref.setReferenceCode(name);
                        ref.setReferenceName(name);
                        references.put(name, ref);
                    }
                    ref.addReferenceColumn(rs.getString("PARENT_COLUMN"), rs.getString("CHILD_COLUMN"));
                }
            }
        }
        tab.getReferences().addAll(references.values());
    }

    @Override
    public SimpleTableInfo getTableMetadata(String tabName) {
        try {
            String schema = resolveSchema();
            SimpleTableInfo tab;
            try (PreparedStatement stmt = dbc.prepareStatement(SQL_TABLES + " AND TABLE_NAME=?")) {
                stmt.setString(1, schema);
                stmt.setString(2, tabName);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    tab = readTableInfo(rs, schema);
                }
            }
            fetchTableDetail(tab);
            return tab;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read Oracle metadata for " + tabName, e);
        }
    }
}
