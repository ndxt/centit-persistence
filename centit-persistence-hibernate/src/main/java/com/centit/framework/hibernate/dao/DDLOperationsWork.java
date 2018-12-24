package com.centit.framework.hibernate.dao;

import com.centit.support.database.ddl.*;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.DBType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DDLOperationsWork implements DDLOperations {

    private BaseDaoImpl<?, ?> baseDao;

    public DDLOperationsWork(){

    }

    public DDLOperationsWork(BaseDaoImpl<?, ?> baseDao){

        this.baseDao = baseDao;
    }

    public void setBaseDao(BaseDaoImpl<?, ?> baseDao) {
        this.baseDao = baseDao;
    }


    private DDLOperations createDDLOpt(Connection connection){

        DBType dbtype = DBType.mapDBType(connection);
        switch (dbtype){
            case Oracle:
                return new OracleDDLOperations(connection);
            case DB2:
                return new DB2DDLOperations(connection);
            case SqlServer:
                return new SqlSvrDDLOperations(connection);
            case MySql:
                return new MySqlDDLOperations(connection);
            case H2:
                return new H2DDLOperations(connection);
            case PostgreSql:
                return new PostgreSqlDDLOperations(connection);

            case Access:
            default:
                throw new RuntimeException("不支持的数据库类型："+dbtype.toString());
        }
    }

    @Override
    public void createSequence(final String sequenceName) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).createSequence(sequenceName));
    }

    @Override
    public void createTable(final TableInfo tableInfo) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).createTable(tableInfo));
    }

    @Override
    public void dropTable(final String tableCode) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).dropTable(tableCode));
    }

    @Override
    public void addColumn(final String tableCode, final TableField column) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).addColumn(tableCode,column));
    }

    @Override
    public void modifyColumn(final String tableCode, final TableField oldColumn, final TableField column) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).modifyColumn(tableCode, oldColumn, column));
    }

    @Override
    public void dropColumn(final String tableCode, final String columnCode) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).dropColumn(tableCode,columnCode));
    }

    @Override
    public void renameColumn(final String tableCode,
                             final String columnCode, final TableField column) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).renameColumn(tableCode,columnCode,column));
    }

    @Override
    public void reconfigurationColumn(final String tableCode,
                                      final String columnCode, final TableField column) throws SQLException {
        baseDao.getCurrentSession().doWork((connection)->
                createDDLOpt(connection).reconfigurationColumn(tableCode,columnCode,column));
    }


    @Override
    public String makeCreateSequenceSql(String sequenceName) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeCreateSequenceSql(sequenceName));
    }

    @Override
    public String makeCreateTableSql(TableInfo tableInfo) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeCreateTableSql(tableInfo));
    }

    @Override
    public String makeDropTableSql(String tableCode) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeDropTableSql(tableCode));
    }

    @Override
    public String makeAddColumnSql(String tableCode, TableField column) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeAddColumnSql(tableCode,column));
    }

    @Override
    public String makeModifyColumnSql(String tableCode, TableField oldColumn,  TableField column) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeModifyColumnSql(tableCode, oldColumn, column));
    }

    @Override
    public String makeDropColumnSql(String tableCode, String columnCode) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeDropColumnSql(tableCode,columnCode));
    }

    @Override
    public String makeRenameColumnSql(String tableCode, String columnCode, TableField column) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeRenameColumnSql(tableCode,columnCode,column));
    }

    @Override
    public List<String> makeReconfigurationColumnSqls(String tableCode, String columnCode, TableField column) {
        return baseDao.getCurrentSession().doReturningWork((connection)->
                createDDLOpt(connection).makeReconfigurationColumnSqls(tableCode,columnCode,column));
    }
}
