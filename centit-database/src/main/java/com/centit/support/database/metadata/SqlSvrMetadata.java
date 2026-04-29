package com.centit.support.database.metadata;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class SqlSvrMetadata implements DatabaseMetadata {
    protected static final Logger logger = LoggerFactory.getLogger(SqlSvrMetadata.class);

    private final static String sqlGetTabColumns =
        "SELECT  a.name, c.name AS typename, a.length , a.xprec, a.xscale, isnullable " +
            "FROM syscolumns a INNER JOIN " +
            "sysobjects b ON a.id = b.id INNER JOIN " +
            "systypes c ON a.xtype = c.xtype " +
            "WHERE b.xtype = 'U' and b.name = ? " +
            "ORDER BY a.colorder";

    private final static String sqlPKName =
        "select a.name,a.object_id, a.parent_object_id ,a.unique_index_id  " +
            "from sys.key_constraints a , sysobjects b " +
            "where a.type='PK' and " +
            " a.parent_object_id=b.id and b.xtype = 'U' and b.name = ? ";

    private final static String sqlPKColumns =
        "select a.name " +
            "from sys.index_columns b join sys.columns a on(a.object_id=b.object_id and a.column_id=b.column_id) " +
            "where b.object_id=? and b.index_id=? " +
            "order by b.key_ordinal";
    //两个参数 均是 integer 对应上面的 parent_object_id 和 unique_index_id


    //foreign_keys
    private final static String sqlFKNames =
        "select a.name,a.object_id,a.parent_object_id , b.name as tabname " +
            "from sys.foreign_keys a join sysobjects b ON a.parent_object_id = b.id " +
            "where referenced_object_id = ? ";
    //参数对应与上面的 parent_object_id 也就是 主表的ID

    //foreign_key_columns
    private final static String sqlFKColumns =
        "SELECT  a.name, c.name AS typename, a.length , a.xprec, a.xscale, isnullable " +
            "FROM syscolumns a INNER JOIN " +
            "sys.foreign_key_columns b ON a.id = b.parent_object_id  and b.parent_column_id=a.colid JOIN " +
            "systypes c ON a.xtype = c.xtype " +
            "WHERE b.constraint_object_id=? " +
            "ORDER BY b.constraint_column_id";
    //参数对应与上面的 object_id

    private String sDBSchema;
    private Connection dbc;

    @Override
    public void setDBConfig(Connection dbc) {
        this.dbc = dbc;
    }

    public String getDBSchema() {
        return sDBSchema;
    }

    public void setDBSchema(String schema) {
        sDBSchema = schema;
    }

    @Override
    public List<SimpleTableInfo> listTables(boolean withColumn, String[] tableNames) {
        List<SimpleTableInfo> tables = new ArrayList<>(100);
        String sql = "SELECT a.name AS TABLE_NAME, a.xtype AS TABLE_TYPE, " +
            "CAST(p.value AS NVARCHAR(MAX)) AS COMMENTS " +
            "FROM sysobjects a " +
            "LEFT JOIN sys.extended_properties p ON p.major_id = a.id AND p.minor_id = 0 AND p.name = 'MS_Description' " +
            "WHERE a.xtype IN ('U ', 'V ')";
        try (PreparedStatement pStmt = dbc.prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) {
            String dbSchema = null;
            try {
                dbSchema = dbc.getSchema();
            } catch (SQLException ignored) {
            }
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                boolean canAddTable = false;
                if (tableNames == null) {
                    canAddTable = true;
                } else {
                    for (String tabName : tableNames) {
                        if (tabName.equalsIgnoreCase(tableName)) {
                            canAddTable = true;
                            break;
                        }
                    }
                }
                if (!canAddTable) {
                    continue;
                }
                SimpleTableInfo tab = new SimpleTableInfo();
                if (dbSchema != null) {
                    tab.setSchema(dbSchema.toUpperCase());
                }
                tab.setTableName(tableName);
                tab.setTableComment(rs.getString("COMMENTS"));
                tab.setTableLabelName(
                    StringUtils.substring(rs.getString("COMMENTS"), 0, 80));
                String tt = rs.getString("TABLE_TYPE").trim();
                tab.setTableType("V".equalsIgnoreCase(tt) ? "V" : "T");
                if (withColumn) {
                    fetchTableDetail(tab);
                }
                tables.add(tab);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        return tables;
    }

    private void fetchTableDetail(SimpleTableInfo tab) {
        String tabName = tab.getTableName();
        int tableId = 0, pkIndId = 0;

        try (PreparedStatement pStmt = dbc.prepareStatement(sqlGetTabColumns)) {
            pStmt.setString(1, tabName);
            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    SimpleTableField field = new SimpleTableField();
                    field.setColumnName(rs.getString("name"));
                    field.setColumnType(rs.getString("typename"));
                    int l = rs.getInt("length");
                    int p = rs.getInt("xprec");
                    field.setMaxLength(p > 0 ? p : l);
                    field.setScale(rs.getInt("xscale"));
                    field.setNullEnable(rs.getString("isnullable"));
                    field.mapToMetadata();
                    tab.addColumn(field);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        try (PreparedStatement pStmt = dbc.prepareStatement(sqlPKName)) {
            pStmt.setString(1, tabName);
            try (ResultSet rs = pStmt.executeQuery()) {
                if (rs.next()) {
                    tab.setPkName(rs.getString("name"));
                    tableId = rs.getInt("parent_object_id");
                    pkIndId = rs.getInt("unique_index_id");
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        if (tableId == 0) {
            return;
        }

        try (PreparedStatement pStmt = dbc.prepareStatement(sqlPKColumns)) {
            pStmt.setInt(1, tableId);
            pStmt.setInt(2, pkIndId);
            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    tab.setColumnAsPrimaryKey(rs.getString("name"));
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        try (PreparedStatement pStmt = dbc.prepareStatement(sqlFKNames)) {
            pStmt.setInt(1, tableId);
            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    SimpleTableReference ref = new SimpleTableReference();
                    ref.setParentTableName(tabName);
                    ref.setTableName(rs.getString("tabname"));
                    ref.setReferenceCode(rs.getString("name"));
                    ref.setObjectId(rs.getInt("object_id"));
                    tab.addReference(ref);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        for (SimpleTableReference ref : tab.getReferences()) {
            try (PreparedStatement pStmt = dbc.prepareStatement(sqlFKColumns)) {
                pStmt.setInt(1, ref.getObjectId());
                try (ResultSet rs = pStmt.executeQuery()) {
                    while (rs.next()) {
                        String columnName = rs.getString("name");
                        ref.addReferenceColumn(columnName, columnName);
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getLocalizedMessage(), e);
            }
        }
    }

    public SimpleTableInfo getTableMetadata(String tabName) {
        SimpleTableInfo tab = new SimpleTableInfo(tabName);
        try {
            tab.setSchema(dbc.getSchema().toUpperCase());
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        fetchTableDetail(tab);

        if (tab.getColumns().isEmpty()) {
            return null;
        }
        return tab;
    }

}
