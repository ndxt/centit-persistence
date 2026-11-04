package com.centit.support.database.metadata;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

/** Strict JDBC evidence. No SQL execution, inferred defaults, or partial-success fallback. */
public final class JdbcTableVerification {
    private JdbcTableVerification() {
    }

    /** Resolve only this native identity using the same rules as complete verification. */
    public static boolean tableExists(Connection connection, String schema, String tableName) throws SQLException {
        return resolveIdentity(connection, schema, tableName) != null;
    }

    public static JSONObject resolveIdentity(Connection connection, String schema, String tableName) throws SQLException {
        if (StringUtils.isBlank(tableName)) {
            throw new SQLException("A native table name is required");
        }
        DatabaseMetaData metadata = connection.getMetaData();
        // PostgreSQL protocol drivers may expose a database name but no JDBC catalog namespace.
        String catalog = metadata.supportsCatalogsInTableDefinitions() ? connection.getCatalog() : null;
        boolean upper = metadata.storesUpperCaseIdentifiers();
        boolean lower = metadata.storesLowerCaseIdentifiers();
        if (upper && lower) {
            throw new SQLException("Conflicting JDBC identifier case rules");
        }
        String identifierCase = upper ? "upper" : lower ? "lower" : "preserved";
        String normalizedName = upper ? tableName.toUpperCase(Locale.ROOT)
                : lower ? tableName.toLowerCase(Locale.ROOT) : tableName;
        JSONObject table = findTable(metadata, catalog, schema, tableName);
        if (!tableName.equals(normalizedName)) {
            JSONObject normalizedTable = findTable(metadata, catalog, schema, normalizedName);
            if (table != null && normalizedTable != null) {
                throw new SQLException("Ambiguous exact and unquoted native table identities");
            }
            if (table == null) {
                table = normalizedTable;
            }
        }
        if (table == null) return null;
        table.put("requestedTableName", tableName);
        table.put("unquotedIdentifierCase", identifierCase);
        table.put("identityResolution", tableName.equals(table.getString("tableName"))
                ? "exact" : "unquoted-case-fold");
        return table;
    }

    public static JSONObject read(Connection connection, String schema, String tableName)
            throws SQLException {
        JSONObject table = resolveIdentity(connection, schema, tableName);
        if (table == null) throw new SQLException("Native table identity was not found");
        DatabaseMetaData metadata = connection.getMetaData();
        DBType dbType = DBType.mapDBType(connection);
        tableName = table.getString("tableName");
        schema = table.getString("schema");
        String catalog = table.getString("catalog");
        List<String> unverified = new ArrayList<>();
        unverified.add("check constraints, triggers and runtime behavior");
        JSONArray columns = new JSONArray();
        Set<String> names = new HashSet<>();
        try (ResultSet rows = metadata.getColumns(catalog, pattern(metadata, schema),
                pattern(metadata, tableName), null)) {
            Set<String> availableColumnLabels = columnLabels(rows.getMetaData());
            while (rows.next()) {
                if (!tableName.equals(rows.getString("TABLE_NAME"))
                        || !Objects.equals(schema, rows.getString("TABLE_SCHEM"))
                        || !Objects.equals(catalog, rows.getString("TABLE_CAT"))) {
                    continue;
                }
                JSONObject column = new JSONObject();
                String name = required(rows, "COLUMN_NAME");
                if (!names.add(name)) {
                    throw new SQLException("Duplicate native column identity");
                }
                String type = required(rows, "TYPE_NAME");
                Integer length = nullableInt(rows, "COLUMN_SIZE");
                Integer scale = nullableInt(rows, "DECIMAL_DIGITS");
                column.put("columnName", name);
                column.put("columnType", type);
                column.put("jdbcType", nullableInt(rows, "DATA_TYPE"));
                column.put("maxLength", length);
                column.put("scale", scale);
                if (length == null) {
                    unverified.add("column length or precision: " + name);
                }
                Integer nullable = nullableInt(rows, "NULLABLE");
                if (nullable != null && (nullable == DatabaseMetaData.columnNoNulls
                        || nullable == DatabaseMetaData.columnNullable)) {
                    column.put("mandatory", nullable == DatabaseMetaData.columnNoNulls);
                } else {
                    unverified.add("column nullability: " + name);
                }
                String defaultValue = rows.getString("COLUMN_DEF");
                JSONObject definition = new JSONObject();
                definition.put("present", defaultValue != null);
                if (defaultValue != null) {
                    definition.put("expression", defaultValue);
                }
                column.put("defaultDefinition", definition);
                column.put("columnOrder", nullableInt(rows, "ORDINAL_POSITION"));
                // Oracle exposes COLUMN_DEF as LONG: consume it before later metadata columns.
                readGenerationFlag(rows, availableColumnLabels, "IS_AUTOINCREMENT",
                        "autoIncrement", name, column, unverified);
                String autoIncrement = column.getString("autoIncrement");
                column.put("fieldType", "YES".equalsIgnoreCase(autoIncrement)
                        ? FieldType.IDENTITY : FieldType.mapToFieldType(type, dbType));
                readGenerationFlag(rows, availableColumnLabels, "IS_GENERATEDCOLUMN",
                        "generatedColumn", name, column, unverified);
                columns.add(column);
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("Native columns are unavailable");
        }
        columns.sort(Comparator.comparing(value -> ((JSONObject) value).getString("columnName")));
        JSONArray primaryKeys = keys(metadata, catalog, schema, tableName, names);
        Set<String> primaryColumns = new HashSet<>();
        for (Object value : primaryKeys) {
            primaryColumns.add(((JSONObject) value).getString("columnName"));
        }
        for (Object value : columns) {
            JSONObject column = (JSONObject) value;
            column.put("primaryKey", primaryColumns.contains(column.getString("columnName")));
        }
        table.put("columns", columns);
        table.put("primaryKeys", primaryKeys);
        table.put("indexes", indexes(metadata, catalog, schema, tableName, names, unverified));
        table.put("importedKeys", foreignKeys(metadata, catalog, schema, tableName, true));
        table.put("exportedKeys", foreignKeys(metadata, catalog, schema, tableName, false));
        table.put("unverified", unverified);
        table.put("source", "jdbc-verification");
        // All cursors completed on one connection; this is not a transactional DDL snapshot.
        return table;
    }

    private static Set<String> columnLabels(ResultSetMetaData metadata) throws SQLException {
        if (metadata == null) {
            throw new SQLException("JDBC columns result metadata is unavailable");
        }
        Set<String> labels = new HashSet<>();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            String label = metadata.getColumnLabel(index);
            if (StringUtils.isNotBlank(label)) {
                labels.add(label.toUpperCase(Locale.ROOT));
            }
        }
        return labels;
    }

    /** Missing generation capabilities remain unknown; failed reads still fail verification. */
    private static void readGenerationFlag(ResultSet rows, Set<String> availableColumnLabels,
                                           String label, String property, String columnName,
                                           JSONObject column, List<String> unverified) throws SQLException {
        if (!availableColumnLabels.contains(label)) {
            unverified.add("column " + property + " (JDBC metadata label unavailable): " + columnName);
            return;
        }
        String value = rows.getString(label);
        column.put(property, value);
        if (!"YES".equals(value) && !"NO".equals(value)) {
            unverified.add("column " + property + " (JDBC value unknown): " + columnName);
        }
    }

    private static JSONObject findTable(DatabaseMetaData metadata, String catalog, String schema,
                                        String name) throws SQLException {
        JSONObject table = null;
        // JDBC arguments are patterns; escaped lookup and exact filtering preserve native identity.
        try (ResultSet rows = metadata.getTables(catalog, pattern(metadata, schema),
                pattern(metadata, name), null)) {
            while (rows.next()) {
                String actualSchema = rows.getString("TABLE_SCHEM");
                String actualCatalog = rows.getString("TABLE_CAT");
                if (!name.equals(rows.getString("TABLE_NAME"))
                        || (schema != null && !schema.equals(actualSchema))
                        || (catalog != null && !catalog.equals(actualCatalog))) {
                    continue;
                }
                if (table != null) {
                    throw new SQLException("Ambiguous native table identity");
                }
                table = new JSONObject();
                table.put("tableName", name);
                table.put("schema", actualSchema);
                table.put("catalog", actualCatalog);
                table.put("tableType", required(rows, "TABLE_TYPE"));
            }
        }
        return table;
    }

    private static JSONArray keys(DatabaseMetaData metadata, String catalog, String schema,
                                  String table, Set<String> columns) throws SQLException {
        JSONArray result = new JSONArray();
        try (ResultSet rows = metadata.getPrimaryKeys(catalog, schema, table)) {
            while (rows.next()) {
                assertTable(rows, "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", catalog, schema, table);
                JSONObject key = new JSONObject();
                String column = required(rows, "COLUMN_NAME");
                if (!columns.contains(column)) {
                    throw new SQLException("Primary key refers to an unread column");
                }
                key.put("columnName", column);
                key.put("name", rows.getString("PK_NAME"));
                key.put("sequence", positiveInt(rows, "KEY_SEQ"));
                result.add(key);
            }
        }
        orderAndValidate(result, value -> "primary");
        return result;
    }

    private static JSONArray indexes(DatabaseMetaData metadata, String catalog, String schema,
                                     String table, Set<String> columns, List<String> unverified)
            throws SQLException {
        JSONArray result = new JSONArray();
        try (ResultSet rows = metadata.getIndexInfo(catalog, schema, table, false, false)) {
            while (rows.next()) {
                assertTable(rows, "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", catalog, schema, table);
                Integer type = nullableInt(rows, "TYPE");
                if (type == null) {
                    throw new SQLException("Index type is unknown");
                }
                if (type == DatabaseMetaData.tableIndexStatistic) {
                    continue;
                }
                JSONObject index = new JSONObject();
                index.put("name", required(rows, "INDEX_NAME"));
                boolean nonUnique = rows.getBoolean("NON_UNIQUE");
                if (rows.wasNull()) {
                    throw new SQLException("Index uniqueness is unknown");
                }
                index.put("unique", !nonUnique);
                index.put("type", type);
                index.put("sequence", positiveInt(rows, "ORDINAL_POSITION"));
                String column = rows.getString("COLUMN_NAME");
                if (column == null || !columns.contains(column)) {
                    unverified.add("index expression: " + index.getString("name"));
                }
                index.put("columnName", column);
                index.put("direction", rows.getString("ASC_OR_DESC"));
                index.put("filter", rows.getString("FILTER_CONDITION"));
                result.add(index);
            }
        }
        orderAndValidate(result, value -> value.getString("name"));
        return result;
    }

    private static JSONArray foreignKeys(DatabaseMetaData metadata, String catalog, String schema,
                                         String table, boolean imported) throws SQLException {
        JSONArray result = new JSONArray();
        try (ResultSet rows = imported ? metadata.getImportedKeys(catalog, schema, table)
                : metadata.getExportedKeys(catalog, schema, table)) {
            while (rows.next()) {
                assertTable(rows, imported ? "FKTABLE_CAT" : "PKTABLE_CAT",
                        imported ? "FKTABLE_SCHEM" : "PKTABLE_SCHEM",
                        imported ? "FKTABLE_NAME" : "PKTABLE_NAME", catalog, schema, table);
                JSONObject key = new JSONObject();
                key.put("name", required(rows, "FK_NAME"));
                key.put("parentCatalog", rows.getString("PKTABLE_CAT"));
                key.put("parentSchema", rows.getString("PKTABLE_SCHEM"));
                key.put("parentTable", required(rows, "PKTABLE_NAME"));
                key.put("parentColumn", required(rows, "PKCOLUMN_NAME"));
                key.put("childCatalog", rows.getString("FKTABLE_CAT"));
                key.put("childSchema", rows.getString("FKTABLE_SCHEM"));
                key.put("childTable", required(rows, "FKTABLE_NAME"));
                key.put("childColumn", required(rows, "FKCOLUMN_NAME"));
                key.put("sequence", positiveInt(rows, "KEY_SEQ"));
                key.put("updateRule", nullableInt(rows, "UPDATE_RULE"));
                key.put("deleteRule", nullableInt(rows, "DELETE_RULE"));
                key.put("deferrability", nullableInt(rows, "DEFERRABILITY"));
                result.add(key);
            }
        }
        orderAndValidate(result, value -> JSONArray.of(value.get("childCatalog"),
                value.get("childSchema"), value.get("childTable"), value.get("name")).toJSONString());
        return result;
    }

    private static void assertTable(ResultSet rows, String catalogKey, String schemaKey,
                                    String tableKey, String catalog, String schema, String table)
            throws SQLException {
        if (!Objects.equals(catalog, rows.getString(catalogKey))
                || !Objects.equals(schema, rows.getString(schemaKey))
                || !table.equals(rows.getString(tableKey))) {
            throw new SQLException("Constraint belongs to a different native table");
        }
    }

    private static void orderAndValidate(JSONArray rows,
                                          java.util.function.Function<JSONObject, String> identity)
            throws SQLException {
        rows.sort(Comparator.comparing((Object value) -> identity.apply((JSONObject) value))
                .thenComparingInt(value -> ((JSONObject) value).getIntValue("sequence")));
        String previous = null;
        int expected = 0;
        for (Object value : rows) {
            JSONObject row = (JSONObject) value;
            String current = identity.apply(row);
            expected = Objects.equals(previous, current) ? expected + 1 : 1;
            if (row.getIntValue("sequence") != expected) {
                throw new SQLException("Constraint columns are duplicated or incomplete");
            }
            previous = current;
        }
    }

    private static String pattern(DatabaseMetaData metadata, String value) throws SQLException {
        String escape = metadata.getSearchStringEscape();
        if (value == null || StringUtils.isEmpty(escape)) {
            return null; // Enumerate and filter exact identities when the driver cannot escape.
        }
        return value.replace(escape, escape + escape).replace("_", escape + "_")
                .replace("%", escape + "%");
    }

    private static String required(ResultSet rows, String key) throws SQLException {
        String value = rows.getString(key);
        if (StringUtils.isBlank(value)) {
            throw new SQLException("Missing JDBC identity or type: " + key);
        }
        return value;
    }

    private static Integer nullableInt(ResultSet rows, String key) throws SQLException {
        int value = rows.getInt(key);
        return rows.wasNull() ? null : value;
    }

    private static int positiveInt(ResultSet rows, String key) throws SQLException {
        Integer value = nullableInt(rows, key);
        if (value == null || value < 1) {
            throw new SQLException("Missing JDBC column sequence: " + key);
        }
        return value;
    }
}
