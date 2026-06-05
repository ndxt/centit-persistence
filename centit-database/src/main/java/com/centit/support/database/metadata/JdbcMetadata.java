package com.centit.support.database.metadata;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcMetadata implements DatabaseMetadata {
    protected static final Logger logger = LoggerFactory.getLogger(JdbcMetadata.class);
    private Connection dbc;
    private String dbSchema;

    @Override
    public void setDBConfig(Connection dbc) {
        this.dbc = dbc;
    }

    @Override
    public List<SimpleTableInfo> listTables(boolean withColumn, String[] tableNames) {
        List<SimpleTableInfo> tables = new ArrayList<>(100);
        String dbSechema = this.getDBSchema();
        String dbCatalog = this.getDBCatalog();
        try {
            DatabaseMetaData dbmd = dbc.getMetaData();
            Set<String> tableNameSet = null;
            if (tableNames != null) {
                tableNameSet = new HashSet<>(tableNames.length * 2);
                for (String name : tableNames) {
                    tableNameSet.add(name.toUpperCase());
                }
            }
            try (ResultSet rs = dbmd.getTables(dbCatalog, dbSechema, null, new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    if (tableNameSet != null && !tableNameSet.contains(rs.getString("TABLE_NAME").toUpperCase())) {
                        continue;
                    }
                    SimpleTableInfo tab = new SimpleTableInfo();
                    if (dbSechema != null) {
                        tab.setSchema(dbSechema.toUpperCase());
                    }
                    tab.setTableName(rs.getString("TABLE_NAME"));
                    tab.setTableComment(rs.getString("REMARKS"));
                    tab.setTableLabelName(
                        StringUtils.substring(tab.getTableComment(), 0, 80));
                    String tt = rs.getString("TABLE_TYPE");
                    if ("view".equalsIgnoreCase(tt) || "table".equalsIgnoreCase(tt)) {
                        if (withColumn) {
                            fetchTableDetail(tab, dbmd);
                        }
                        tab.setTableType("view".equalsIgnoreCase(tt) ? "V" : "T");
                        tables.add(tab);
                    }
                }
            }
        }catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        return tables;
    }

    private void fetchTableDetail(SimpleTableInfo tab, DatabaseMetaData dbmd) {
        String tabName = tab.getTableName();
        try {
            String dbSchema = this.getDBSchema();
            String dbCatalog = this.getDBCatalog();

            ResultSet rs = dbmd.getColumns(dbCatalog, dbSchema, tabName, null);
            while (rs.next()) {
                SimpleTableField field = new SimpleTableField();
                field.setColumnName(rs.getString("COLUMN_NAME"));
                field.setColumnType(rs.getString("TYPE_NAME"));
                field.setMaxLength(rs.getInt("COLUMN_SIZE"));
                field.setScale(rs.getInt("DECIMAL_DIGITS"));
                field.setNullEnable(rs.getString("NULLABLE"));
                field.setColumnComment(rs.getString("REMARKS"));
                field.setFieldLabelName(
                    StringUtils.substring(field.getColumnComment(),0, 80));
                field.mapToMetadata();
                tab.addColumn(field);
            }
            rs.close();
            rs = dbmd.getPrimaryKeys(dbCatalog, dbSchema, tabName);
            while (rs.next()) {
                tab.setColumnAsPrimaryKey(rs.getString("COLUMN_NAME"));
                tab.setPkName(rs.getString("PK_NAME"));
            }
            rs.close();

            rs = dbmd.getExportedKeys(dbCatalog, dbSchema, tabName);
            Map<String, SimpleTableReference> referenceHashMap = new HashMap<>();
            while (rs.next()) {
                String fkTableName = rs.getString("FKTABLE_NAME");
                SimpleTableReference ref = referenceHashMap.get(fkTableName);
                if (ref == null) {
                    ref = new SimpleTableReference();
                    ref.setTableName(fkTableName);
                    ref.setParentTableName(tabName);
                    ref.setReferenceCode(rs.getString("FK_NAME"));
                }
                ref.addReferenceColumn(rs.getString("PKCOLUMN_NAME"),
                    rs.getString("FKCOLUMN_NAME"));
                referenceHashMap.put(fkTableName, ref);
            }
            rs.close();

            for (Map.Entry<String, SimpleTableReference> entry : referenceHashMap.entrySet()) {
                tab.addReference(entry.getValue());
            }

        } catch (SQLException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
        }
    }

    /**
     * 没有获取外键
     */
    @Override
    public SimpleTableInfo getTableMetadata(String tabName) {
        SimpleTableInfo tab = new SimpleTableInfo(tabName);
        try {
            String dbSechema = this.getDBSchema();
            String dbCatalog = this.getDBCatalog();
            DatabaseMetaData dbmd = dbc.getMetaData();
            ResultSet rs = dbmd.getTables(dbCatalog, dbSechema, tabName, null);
            if (!rs.next()) {
                return null;
            }
            tab.setTableLabelName(rs.getString("REMARKS"));
            String tt = rs.getString("TABLE_TYPE");
            if ("view".equalsIgnoreCase(tt) || "table".equalsIgnoreCase(tt)) {
                tab.setTableType("view".equalsIgnoreCase(tt) ? "V" : "T");
            }
            rs.close();

            fetchTableDetail(tab, dbmd);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
        }
        return tab;
    }

    @Override
    public String getDBSchema() {
        if (StringUtils.isNotBlank(this.dbSchema)) {
            return this.dbSchema;
        }
        try {
            return dbc.getSchema();
        } catch (AbstractMethodError error) {
            return null;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
            return null;
        }
    }

    @Override
    public void setDBSchema(String schema) {
        this.dbSchema = schema;
    }

    public String getDBCatalog() {
        try {
            return dbc.getCatalog();
        } catch (AbstractMethodError error) {
            return null;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
            return null;
        }
    }
}
