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
import java.util.Locale;
import java.util.Map;

public class DB2Metadata implements DatabaseMetadata {

    private static final String SQL_TABLES =
        "SELECT NAME, TYPE, REMARKS FROM sysibm.systables WHERE CREATOR=? AND TYPE IN ('T', 'V')";

    private static final String SQL_COLUMNS =
        "SELECT a.NAME, a.COLTYPE, a.LENGTH, a.SCALE, a.NULLS, a.REMARKS, " +
            "CASE WHEN b.TYPE IN ('T', 'G') THEN a.DEFAULT ELSE 'N' END AS DEFAULT_KIND, a.DEFAULTVALUE " +
            "FROM sysibm.syscolumns a JOIN sysibm.systables b " +
            "ON b.NAME=a.TBNAME AND b.CREATOR=a.TBCREATOR " +
            "WHERE a.TBCREATOR=? AND a.TBNAME=? ORDER BY a.COLNO";

    private static final String SQL_PRIMARY_KEY =
        "SELECT k.CONSTNAME, k.COLNAME, k.COLSEQ FROM sysibm.syskeycoluse k " +
            "JOIN sysibm.systabconst c ON c.TBCREATOR=k.TBCREATOR AND c.TBNAME=k.TBNAME " +
            "AND c.CONSTNAME=k.CONSTNAME " +
            "WHERE k.TBCREATOR=? AND k.TBNAME=? AND c.TYPE='P' ORDER BY k.COLSEQ";

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
            throw new IllegalStateException("Failed to determine DB2 schema", e);
        }
    }

    @Override
    public void setDBSchema(String schema) {
        sDBSchema = schema == null ? null : schema.toUpperCase(Locale.ROOT);
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
            throw new SQLException("Cannot determine DB2 schema; setDBSchema is required");
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
                        String name = rs.getString("NAME");
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
            throw new IllegalStateException("Failed to read DB2 table metadata", e);
        }
    }

    private SimpleTableInfo readTableInfo(ResultSet rs, String schema) throws SQLException {
        SimpleTableInfo tab = new SimpleTableInfo(rs.getString("NAME"));
        tab.setSchema(schema);
        tab.setTableComment(rs.getString("REMARKS"));
        tab.setTableLabelName(StringUtils.substring(tab.getTableComment(), 0, 80));
        tab.setTableType("V".equalsIgnoreCase(rs.getString("TYPE")) ? "V" : "T");
        return tab;
    }

    private void fetchTableDetail(SimpleTableInfo tab) throws SQLException {
        try (PreparedStatement stmt = dbc.prepareStatement(SQL_COLUMNS)) {
            stmt.setString(1, tab.getSchema());
            stmt.setString(2, tab.getTableName());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    SimpleTableField field = new SimpleTableField();
                    field.setColumnName(rs.getString("NAME"));
                    field.setColumnType(rs.getString("COLTYPE"));
                    field.setMaxLength(rs.getInt("LENGTH"));
                    field.setScale(rs.getInt("SCALE"));
                    field.setNullEnable(rs.getString("NULLS"));
                    field.setColumnComment(rs.getString("REMARKS"));
                    field.setFieldLabelName(StringUtils.substring(field.getColumnComment(), 0, 80));
                    field.mapToMetadata();
                    String defaultKind = rs.getString("DEFAULT_KIND");
                    if ("I".equals(defaultKind) || "J".equals(defaultKind)) {
                        field.setFieldType(FieldType.IDENTITY);
                    } else {
                        field.setDefaultValue(readDefaultValue(defaultKind, rs.getString("DEFAULTVALUE"), field));
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
                    tab.setPkName(rs.getString("CONSTNAME"));
                    SimpleTableField field = tab.findFieldByColumn(rs.getString("COLNAME"));
                    if (field == null) {
                        throw new SQLException("Primary key column is absent from " + tab.getTableName());
                    }
                    field.setPrimaryKey(true);
                    field.setPrimaryKeyOrder(rs.getInt("COLSEQ"));
                }
            }
        }
        // Use JDBC for FK columns rather than splitting catalog strings with platform-dependent formats.
        Map<String, SimpleTableReference> references = new LinkedHashMap<>();
        try (ResultSet rs = dbc.getMetaData().getExportedKeys(dbc.getCatalog(), tab.getSchema(), tab.getTableName())) {
            while (rs.next()) {
                if (!tab.getTableName().equals(rs.getString("PKTABLE_NAME"))
                    || (rs.getString("PKTABLE_SCHEM") != null && !tab.getSchema().equals(rs.getString("PKTABLE_SCHEM")))
                    || (rs.getString("FKTABLE_SCHEM") != null && !tab.getSchema().equals(rs.getString("FKTABLE_SCHEM")))) {
                    continue;
                }
                String name = rs.getString("FK_NAME");
                if (StringUtils.isBlank(name)) {
                    throw new SQLException("Cannot identify unnamed DB2 foreign key on " + rs.getString("FKTABLE_NAME"));
                }
                String key = rs.getString("FKTABLE_NAME") + '\0' + name;
                SimpleTableReference ref = references.get(key);
                if (ref == null) {
                    ref = new SimpleTableReference();
                    ref.setParentTableName(tab.getTableName());
                    ref.setTableName(rs.getString("FKTABLE_NAME"));
                    ref.setReferenceCode(name);
                    ref.setReferenceName(name);
                    references.put(key, ref);
                }
                ref.addReferenceColumn(rs.getString("PKCOLUMN_NAME"), rs.getString("FKCOLUMN_NAME"));
            }
        }
        tab.getReferences().addAll(references.values());
    }

    private String readDefaultValue(String kind, String value, SimpleTableField field) {
        if (kind == null) {
            return null;
        }
        return switch (kind) {
            case "1" -> "'" + value.replace("'", "''") + "'";
            case "2", "3", "4", "9", "a", "b" -> StringUtils.trimToNull(value);
            case "5" -> "X'" + value + "'";
            case "6" -> "UX'" + value + "'";
            case "7", "8" -> "G'" + value.replace("'", "''") + "'";
            case "S" -> "CURRENT SQLID";
            case "U" -> "SESSION_USER";
            case "Y" -> field.isMandatory() ? implicitDefaultValue(field) : "NULL";
            case "B" -> implicitDefaultValue(field);
            default -> null;
        };
    }

    private String implicitDefaultValue(SimpleTableField field) {
        String type = field.getColumnType().toUpperCase(Locale.ROOT);
        return switch (type) {
            case "DATE" -> "CURRENT DATE";
            case "TIME" -> "CURRENT TIME";
            case "TIMESTMP", "TIMESTAMP" -> "CURRENT TIMESTAMP";
            case "CHAR" -> "'" + " ".repeat(field.getMaxLength()) + "'";
            case "VARCHAR", "LONGVAR" -> "''";
            case "GRAPHIC" -> "G'" + " ".repeat(field.getMaxLength()) + "'";
            case "VARG", "LONGVARG" -> "G''";
            case "BINARY" -> "X'" + "00".repeat(field.getMaxLength()) + "'";
            case "VARBIN", "VARBINARY" -> "X''";
            default -> "0";
        };
    }

    @Override
    public SimpleTableInfo getTableMetadata(String tabName) {
        try {
            String schema = resolveSchema();
            SimpleTableInfo tab;
            try (PreparedStatement stmt = dbc.prepareStatement(SQL_TABLES + " AND NAME=?")) {
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
            throw new IllegalStateException("Failed to read DB2 metadata for " + tabName, e);
        }
    }
}
