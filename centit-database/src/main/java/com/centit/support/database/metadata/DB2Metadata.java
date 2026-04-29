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

public class DB2Metadata implements DatabaseMetadata {

    protected static final Logger logger = LoggerFactory.getLogger(DB2Metadata.class);
    private final static String sqlGetTabColumns =
        "select a.name,a.coltype,a.length, a.scale, a.nulls " +
            "from sysibm.systables b , sysibm.syscolumns a " +
            "where a.tbcreator= ? and a.tbname= ? " +
            "and b.name=a.tbname and b.creator=a.tbcreator";

    private final static String sqlPKInfo =
        "select constname, colname " +
            "from sysibm.syskeycoluse " +
            "where tbcreator=? and tbname=? " +
            "order by colseq";

    private final static String sqlFKInfo =
        "select tbname, relname, colcount, fkcolnames, pkcolnames " +
            "from sysibm.sysrels " +
            "where refkeyname= ?";

    private final static String sqlFKColumn =
        "select a.name,a.coltype,a.length, a.scale, a.nulls " +
            "from sysibm.systables b , sysibm.syscolumns a " +
            "where a.tbcreator= ? and a.tbname= ? and a.name= ? " +
            "and b.name=a.tbname and b.creator=a.tbcreator";

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
        if (schema != null)
            sDBSchema = schema.toUpperCase();
    }

    @Override
    public List<SimpleTableInfo> listTables(boolean withColumn, String[] tableNames) {
        List<SimpleTableInfo> tables = new ArrayList<>(100);
        String sql = "SELECT NAME, TYPE, REMARKS FROM sysibm.systables WHERE CREATOR=? AND TYPE IN ('T', 'V')";
        try (PreparedStatement pStmt = dbc.prepareStatement(sql)) {
            pStmt.setString(1, sDBSchema);
            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("NAME");
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
                    if (sDBSchema != null) {
                        tab.setSchema(sDBSchema.toUpperCase());
                    }
                    tab.setTableName(tableName);
                    tab.setTableComment(rs.getString("REMARKS"));
                    tab.setTableLabelName(
                        StringUtils.substring(rs.getString("REMARKS"), 0, 80));
                    String tt = rs.getString("TYPE");
                    tab.setTableType("V".equalsIgnoreCase(tt) ? "V" : "T");
                    if (withColumn) {
                        fetchTableDetail(tab);
                    }
                    tables.add(tab);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
        return tables;
    }

    private void fetchTableDetail(SimpleTableInfo tab) {
        String tabName = tab.getTableName();
        try (PreparedStatement pStmt = dbc.prepareStatement(sqlGetTabColumns)) {
            pStmt.setString(1, sDBSchema);
            pStmt.setString(2, tabName);
            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    SimpleTableField field = new SimpleTableField();
                    field.setColumnName(rs.getString("name"));
                    field.setColumnType(rs.getString("coltype"));
                    field.setMaxLength(rs.getInt("length"));
                    field.setScale(rs.getInt("scale"));
                    field.setNullEnable(rs.getString("nulls"));
                    field.mapToMetadata();
                    tab.addColumn(field);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        try (PreparedStatement pStmt = dbc.prepareStatement(sqlPKInfo)) {
            pStmt.setString(1, sDBSchema);
            pStmt.setString(2, tabName);
            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    tab.setPkName(rs.getString("constname"));
                    tab.setColumnAsPrimaryKey(rs.getString("colname"));
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        if (tab.getPkName() == null) {
            return;
        }

        try (PreparedStatement pStmt = dbc.prepareStatement(sqlFKInfo)) {
            pStmt.setString(1, tab.getPkName());
            try (ResultSet rs = pStmt.executeQuery()) {
                while (rs.next()) {
                    SimpleTableReference ref = new SimpleTableReference();
                    ref.setParentTableName(tabName);
                    ref.setTableName(rs.getString("tbname"));
                    ref.setReferenceCode(rs.getString("relname"));
                    int nColCount = rs.getInt("colcount");
                    String sFColNames = rs.getString("fkcolnames").trim();
                    String[] p = sFColNames.split("\\s+");
                    String sPColNames = rs.getString("pkcolnames").trim();
                    String[] pK = sPColNames.split("\\s+");
                    if (nColCount != p.length) {
                        logger.warn("外键{}字段分隔出错！", ref.getReferenceCode());
                    }
                    for (int i = 0; i < p.length; i++) {
                        if (i < pK.length)
                            ref.addReferenceColumn(pK[i], p[i]);
                    }
                    tab.addReference(ref);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
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
