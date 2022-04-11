package com.centit.framework.jdbc.dao;

import com.centit.support.database.ddl.DDLOperations;
import com.centit.support.database.ddl.GeneralDDLOperations;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

@SuppressWarnings("unused")
public class DDLOperationsWork implements DDLOperations {

    protected static Logger logger = LoggerFactory.getLogger(DDLOperations.class);

    private BaseDaoImpl<?, ?> baseDao;

    public DDLOperationsWork(){

    }

    public DDLOperationsWork(BaseDaoImpl<?, ?> baseDao){

        this.baseDao = baseDao;
    }

    public void setBaseDao(BaseDaoImpl<?, ?> baseDao) {
        this.baseDao = baseDao;
    }

    private DDLOperations operations = null;
    public DDLOperations getDDLOperations() throws SQLException {
        if(operations==null)
            operations = GeneralDDLOperations.createDDLOperations(
                    baseDao.getConnection());
        return operations;
    }

    @Override
    public void createSequence(final String sequenceName) throws SQLException {
        getDDLOperations().createSequence(sequenceName);
    }

    @Override
    public String makeCreateViewSql(String selectSql, String viewName) {
        try {
            return getDDLOperations().makeCreateViewSql(selectSql, viewName);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void createTable(final TableInfo tableInfo) throws SQLException {
        getDDLOperations().createTable(tableInfo);
    }

    @Override
    public void dropTable(final String tableCode) throws SQLException {
        getDDLOperations().dropTable(tableCode);
    }

    @Override
    public void addColumn(final String tableCode, final TableField column) throws SQLException {
        getDDLOperations().addColumn(tableCode,column);
    }

    @Override
    public void modifyColumn(final String tableCode, final TableField oldColumn, final TableField column) throws SQLException {
        getDDLOperations().modifyColumn(tableCode, oldColumn, column);
    }

    @Override
    public void dropColumn(final String tableCode, final String columnCode) throws SQLException {
        getDDLOperations().dropColumn(tableCode,columnCode);
    }

    @Override
    public void renameColumn(final String tableCode, final String columnCode, final TableField column) throws SQLException {
        getDDLOperations().renameColumn(tableCode,columnCode,column);
    }

    @Override
    public void reconfigurationColumn(final String tableCode, final String columnCode,
            final TableField column) throws SQLException {
        getDDLOperations().
            reconfigurationColumn(tableCode,columnCode,column);
    }


    @Override
    public String makeCreateSequenceSql(String sequenceName) {
        try {
            return getDDLOperations().makeCreateSequenceSql(sequenceName);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String makeCreateTableSql(TableInfo tableInfo) {
        try {
            return getDDLOperations().makeCreateTableSql(tableInfo);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String makeDropTableSql(String tableCode) {
        try {
            return getDDLOperations().makeDropTableSql(tableCode);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String makeAddColumnSql(String tableCode, TableField column) {
        try {
            return getDDLOperations().makeAddColumnSql(tableCode,column);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String makeModifyColumnSql(String tableCode, TableField oldColumn, TableField column) {
        try {
            return getDDLOperations().makeModifyColumnSql(tableCode, oldColumn, column);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String makeDropColumnSql(String tableCode, String columnCode) {
        try {
            return getDDLOperations().makeDropColumnSql(tableCode,columnCode);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public String makeRenameColumnSql(String tableCode, String columnCode, TableField column) {
        try {
            return getDDLOperations().makeRenameColumnSql(tableCode,columnCode,column);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    @Override
    public List<String> makeReconfigurationColumnSqls(String tableCode, String columnCode, TableField column) {
        try {
            return getDDLOperations().makeReconfigurationColumnSqls(tableCode,columnCode,column);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return null;
        }
    }
}
