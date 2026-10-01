package com.centit.support.database.metadata;

import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqlSvrMetadata implements DatabaseMetadata {

    private static final String SQL_TABLES =
        "SELECT o.name AS TABLE_NAME, o.type AS TABLE_TYPE, o.object_id, " +
            "CAST(p.value AS NVARCHAR(MAX)) AS COMMENTS FROM sys.objects o " +
            "JOIN sys.schemas s ON s.schema_id=o.schema_id " +
            "LEFT JOIN sys.extended_properties p " +
            "ON p.class=1 AND p.major_id=o.object_id AND p.minor_id=0 AND p.name='MS_Description' " +
            "WHERE s.name=? AND o.type IN ('U', 'V') AND o.is_ms_shipped=0";

    private static final String SQL_COLUMNS =
        "SELECT a.name, t.name AS typename, COALESCE(b.name, t.name) AS base_typename, " +
            "a.max_length, a.precision, a.scale, a.is_nullable, a.is_identity, d.definition AS default_value, " +
            "CAST(p.value AS NVARCHAR(MAX)) AS COMMENTS " +
            "FROM sys.columns a JOIN sys.types t ON a.user_type_id=t.user_type_id " +
            "LEFT JOIN sys.types b ON a.system_type_id=b.user_type_id AND b.is_user_defined=0 " +
            "LEFT JOIN sys.default_constraints d ON d.object_id=a.default_object_id " +
            "LEFT JOIN sys.extended_properties p ON p.class=1 AND p.major_id=a.object_id " +
            "AND p.minor_id=a.column_id AND p.name='MS_Description' " +
            "WHERE a.object_id=? ORDER BY a.column_id";

    private static final String SQL_PRIMARY_KEY =
        "SELECT k.name AS PK_NAME, c.name AS COLUMN_NAME, i.key_ordinal " +
            "FROM sys.key_constraints k JOIN sys.index_columns i " +
            "ON i.object_id=k.parent_object_id AND i.index_id=k.unique_index_id " +
            "JOIN sys.columns c ON c.object_id=i.object_id AND c.column_id=i.column_id " +
            "WHERE k.parent_object_id=? AND k.type='PK' AND i.key_ordinal>0 ORDER BY i.key_ordinal";

    private static final String SQL_REFERENCES =
        "SELECT fk.name AS FK_NAME, child.name AS TABLE_NAME, " +
            "p.name AS PARENT_COLUMN, c.name AS CHILD_COLUMN " +
            "FROM sys.foreign_keys fk JOIN sys.foreign_key_columns f ON f.constraint_object_id=fk.object_id " +
            "JOIN sys.objects child ON child.object_id=fk.parent_object_id " +
            "JOIN sys.schemas s ON s.schema_id=child.schema_id " +
            "JOIN sys.columns p ON p.object_id=f.referenced_object_id AND p.column_id=f.referenced_column_id " +
            "JOIN sys.columns c ON c.object_id=f.parent_object_id AND c.column_id=f.parent_column_id " +
            "WHERE fk.referenced_object_id=? AND s.name=? ORDER BY fk.name, f.constraint_column_id";

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
            throw new IllegalStateException("Failed to determine SQL Server schema", e);
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
            try (PreparedStatement stmt = dbc.prepareStatement("SELECT SCHEMA_NAME() AS SCHEMA_NAME");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    schema = rs.getString("SCHEMA_NAME");
                }
            }
        }
        if (StringUtils.isBlank(schema)) {
            throw new SQLException("Cannot determine SQL Server schema; setDBSchema is required");
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
                            fetchTableDetail(tab, rs.getInt("object_id"));
                        }
                        tables.add(tab);
                    }
                }
            }
            return tables;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read SQL Server table metadata", e);
        }
    }

    private SimpleTableInfo readTableInfo(ResultSet rs, String schema) throws SQLException {
        SimpleTableInfo tab = new SimpleTableInfo(rs.getString("TABLE_NAME"));
        tab.setSchema(schema);
        tab.setTableComment(rs.getString("COMMENTS"));
        tab.setTableLabelName(StringUtils.substring(tab.getTableComment(), 0, 80));
        tab.setTableType("V".equalsIgnoreCase(StringUtils.trim(rs.getString("TABLE_TYPE"))) ? "V" : "T");
        return tab;
    }

    private void fetchTableDetail(SimpleTableInfo tab, int tableId) throws SQLException {
        try (PreparedStatement stmt = dbc.prepareStatement(SQL_COLUMNS)) {
            stmt.setInt(1, tableId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    SimpleTableField field = new SimpleTableField();
                    field.setColumnName(rs.getString("name"));
                    field.setColumnType(rs.getString("typename"));
                    String baseType = rs.getString("base_typename");
                    int length = rs.getInt("max_length");
                    int precision = rs.getInt("precision");
                    if (length > 0 && ("nchar".equalsIgnoreCase(baseType) || "nvarchar".equalsIgnoreCase(baseType))) {
                        length /= 2;
                    }
                    field.setMaxLength(precision > 0 ? precision : length);
                    field.setScale(rs.getInt("scale"));
                    field.setMandatory(!rs.getBoolean("is_nullable"));
                    field.setColumnComment(rs.getString("COMMENTS"));
                    field.setFieldLabelName(StringUtils.substring(field.getColumnComment(), 0, 80));
                    field.mapToMetadata();
                    // Preserve the actual alias type in columnType, and map its underlying built-in type.
                    field.setFieldType(FieldType.mapToFieldType(baseType));
                    if (rs.getBoolean("is_identity")) {
                        field.setFieldType(FieldType.IDENTITY);
                    } else {
                        field.setDefaultValue(rs.getString("default_value"));
                    }
                    field.setLazyFetch(FieldType.TEXT.equals(field.getFieldType())
                        || FieldType.BYTE_ARRAY.equals(field.getFieldType())
                        || FieldType.JSON_OBJECT.equals(field.getFieldType()));
                    tab.addColumn(field);
                }
            }
        }
        try (PreparedStatement stmt = dbc.prepareStatement(SQL_PRIMARY_KEY)) {
            stmt.setInt(1, tableId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tab.setPkName(rs.getString("PK_NAME"));
                    SimpleTableField field = tab.findFieldByColumn(rs.getString("COLUMN_NAME"));
                    if (field == null) {
                        throw new SQLException("Primary key column is absent from " + tab.getTableName());
                    }
                    field.setPrimaryKey(true);
                    field.setPrimaryKeyOrder(rs.getInt("key_ordinal"));
                }
            }
        }
        Map<String, SimpleTableReference> references = new LinkedHashMap<>();
        try (PreparedStatement stmt = dbc.prepareStatement(SQL_REFERENCES)) {
            stmt.setInt(1, tableId);
            stmt.setString(2, tab.getSchema());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("FK_NAME");
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
            int tableId;
            try (PreparedStatement stmt = dbc.prepareStatement(SQL_TABLES + " AND o.name=?")) {
                stmt.setString(1, schema);
                stmt.setString(2, tabName);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    tab = readTableInfo(rs, schema);
                    tableId = rs.getInt("object_id");
                }
            }
            fetchTableDetail(tab, tableId);
            return tab;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read SQL Server metadata for " + tabName, e);
        }
    }
}
