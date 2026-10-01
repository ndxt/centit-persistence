package com.centit.support.database.metadata;

import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class JdbcMetadata implements DatabaseMetadata {
    private Connection dbc;
    private String dbSchema;

    @Override
    public void setDBConfig(Connection dbc) {
        this.dbc = dbc;
    }

    @Override
    public List<SimpleTableInfo> listTables(boolean withColumn, String[] tableNames) {
        List<SimpleTableInfo> tables = new ArrayList<>(100);
        String schema = getDBSchema();
        String catalog = getDBCatalog();
        try {
            DatabaseMetaData dbmd = dbc.getMetaData();
            Set<String> tableNameSet = null;
            if (tableNames != null) {
                tableNameSet = new HashSet<>(tableNames.length * 2);
                for (String name : tableNames) {
                    if (StringUtils.isBlank(name)) {
                        throw new IllegalArgumentException("表名不能为空");
                    }
                    tableNameSet.add(name.toUpperCase(Locale.ROOT));
                }
            }
            try (ResultSet rs = dbmd.getTables(catalog, metadataPattern(dbmd, schema), null,
                new String[]{"TABLE", "BASE TABLE", "VIEW"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if ((schema != null && !schema.equals(rs.getString("TABLE_SCHEM")))
                        || (tableNameSet != null && !tableNameSet.contains(tableName.toUpperCase(Locale.ROOT)))) {
                        continue;
                    }
                    SimpleTableInfo tab = readTableInfo(rs);
                    if (withColumn) {
                        fetchTableDetail(tab, dbmd, rs.getString("TABLE_CAT"));
                    }
                    tables.add(tab);
                }
            }
            return tables;
        } catch (SQLException e) {
            throw new IllegalStateException("读取数据库表列表失败", e);
        }
    }

    private SimpleTableInfo readTableInfo(ResultSet rs) throws SQLException {
        SimpleTableInfo tab = new SimpleTableInfo(rs.getString("TABLE_NAME"));
        tab.setSchema(rs.getString("TABLE_SCHEM"));
        tab.setTableComment(rs.getString("REMARKS"));
        tab.setTableLabelName(StringUtils.substring(tab.getTableComment(), 0, 80));
        String type = rs.getString("TABLE_TYPE");
        if ("VIEW".equalsIgnoreCase(type)) {
            tab.setTableType("V");
        } else if ("TABLE".equalsIgnoreCase(type) || "BASE TABLE".equalsIgnoreCase(type)) {
            tab.setTableType("T");
        } else {
            throw new UnsupportedOperationException("不支持的表类型: " + type);
        }
        return tab;
    }

    private String metadataPattern(DatabaseMetaData dbmd, String name) throws SQLException {
        if (name == null) {
            return null;
        }
        String escape = dbmd.getSearchStringEscape();
        if (StringUtils.isEmpty(escape)) {
            return name;
        }
        return name.replace(escape, escape + escape)
            .replace("_", escape + "_").replace("%", escape + "%");
    }

    private void fetchTableDetail(SimpleTableInfo tab, DatabaseMetaData dbmd, String catalog) throws SQLException {
        String tableName = tab.getTableName();
        String schema = tab.getSchema();
        try (ResultSet rs = dbmd.getColumns(catalog, metadataPattern(dbmd, schema),
            metadataPattern(dbmd, tableName), null)) {
            while (rs.next()) {
                if (!tableName.equals(rs.getString("TABLE_NAME"))
                    || !Objects.equals(schema, rs.getString("TABLE_SCHEM"))
                    || !Objects.equals(catalog, rs.getString("TABLE_CAT"))) {
                    continue;
                }
                SimpleTableField field = new SimpleTableField();
                field.setColumnName(rs.getString("COLUMN_NAME"));
                String columnType = rs.getString("TYPE_NAME");
                // H2 2.x 返回 SQL 标准名称，统一到 DDL 使用的常用名称。
                if ("CHARACTER VARYING".equalsIgnoreCase(columnType)) {
                    columnType = "VARCHAR";
                }
                field.setColumnType(columnType);
                field.setMaxLength(rs.getInt("COLUMN_SIZE"));
                field.setScale(rs.getInt("DECIMAL_DIGITS"));
                field.setMandatory(rs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls);
                field.setDefaultValue(rs.getString("COLUMN_DEF"));
                field.setColumnComment(rs.getString("REMARKS"));
                field.setFieldLabelName(StringUtils.substring(field.getColumnComment(), 0, 80));
                field.mapToMetadata();
                if ("YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT"))) {
                    field.setFieldType(FieldType.IDENTITY);
                    field.setDefaultValue(null);
                }
                tab.addColumn(field);
            }
        }
        try (ResultSet rs = dbmd.getPrimaryKeys(catalog, schema, tableName)) {
            while (rs.next()) {
                SimpleTableField field = tab.findFieldByColumn(rs.getString("COLUMN_NAME"));
                if (field == null) {
                    throw new IllegalStateException("主键列不存在: " + tableName + "." + rs.getString("COLUMN_NAME"));
                }
                field.setPrimaryKey(true);
                field.setPrimaryKeyOrder(rs.getInt("KEY_SEQ"));
                tab.setPkName(rs.getString("PK_NAME"));
            }
        }
        Map<List<String>, SimpleTableReference> references = new LinkedHashMap<>();
        try (ResultSet rs = dbmd.getExportedKeys(catalog, schema, tableName)) {
            while (rs.next()) {
                String fkTableName = rs.getString("FKTABLE_NAME");
                String fkName = rs.getString("FK_NAME");
                if (StringUtils.isBlank(fkName)) {
                    throw new UnsupportedOperationException("无法区分未命名的外键约束: " + fkTableName);
                }
                List<String> key = Arrays.asList(rs.getString("FKTABLE_CAT"),
                    rs.getString("FKTABLE_SCHEM"), fkTableName, fkName);
                SimpleTableReference ref = references.get(key);
                if (ref == null) {
                    ref = new SimpleTableReference();
                    ref.setTableName(fkTableName);
                    ref.setParentTableName(tableName);
                    ref.setReferenceCode(fkName);
                    references.put(key, ref);
                }
                ref.addReferenceColumn(rs.getString("PKCOLUMN_NAME"), rs.getString("FKCOLUMN_NAME"));
            }
        }
        tab.getReferences().addAll(references.values());
    }

    @Override
    public SimpleTableInfo getTableMetadata(String tableName) {
        if (StringUtils.isBlank(tableName)) {
            throw new IllegalArgumentException("表名不能为空");
        }
        String schema = getDBSchema();
        String catalog = getDBCatalog();
        try {
            DatabaseMetaData dbmd = dbc.getMetaData();
            SimpleTableInfo tab = null;
            String tableCatalog = null;
            try (ResultSet rs = dbmd.getTables(catalog, metadataPattern(dbmd, schema),
                metadataPattern(dbmd, tableName), null)) {
                while (rs.next()) {
                    if (!tableName.equals(rs.getString("TABLE_NAME"))
                        || (schema != null && !schema.equals(rs.getString("TABLE_SCHEM")))) {
                        continue;
                    }
                    if (tab != null) {
                        throw new IllegalStateException("表名对应多个模式，请指定 schema: " + tableName);
                    }
                    tab = readTableInfo(rs);
                    tableCatalog = rs.getString("TABLE_CAT");
                }
            }
            if (tab == null) {
                return null;
            }
            fetchTableDetail(tab, dbmd, tableCatalog);
            return tab;
        } catch (SQLException e) {
            throw new IllegalStateException("读取表元数据失败: " + tableName, e);
        }
    }

    @Override
    public String getDBSchema() {
        if (StringUtils.isNotBlank(dbSchema)) {
            return dbSchema;
        }
        try {
            return dbc.getSchema();
        } catch (AbstractMethodError error) {
            throw new UnsupportedOperationException("JDBC 驱动不支持读取 schema", error);
        } catch (SQLException e) {
            throw new IllegalStateException("读取数据库 schema 失败", e);
        }
    }

    @Override
    public void setDBSchema(String schema) {
        dbSchema = schema;
    }

    public String getDBCatalog() {
        try {
            return dbc.getCatalog();
        } catch (AbstractMethodError error) {
            throw new UnsupportedOperationException("JDBC 驱动不支持读取 catalog", error);
        } catch (SQLException e) {
            throw new IllegalStateException("读取数据库 catalog 失败", e);
        }
    }
}
