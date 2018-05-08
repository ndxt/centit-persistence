package com.centit.framework.mybatis.dao;

import com.centit.support.database.ddl.*;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.DBType;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DDLOperationsWork implements DDLOperations {

    private SqlSession sqlSession;

    public DDLOperationsWork(){

    }

    public DDLOperationsWork(SqlSession sqlSession){

        this.sqlSession = sqlSession;
    }

    public void setBaseDao(SqlSession sqlSession) {
        this.sqlSession = sqlSession;
    }

    /**
     * 可以执行DDL操作
     * @param connection Connection
     * @return 可以执行DDL操作
     */
    public static DDLOperations createDDLOpt(Connection connection){
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
            default:
                return new OracleDDLOperations(connection);
            //throw new  SQLException("不支持的数据库类型："+dbtype.toString());
        }
    }

    private DDLOperations operations = null;
    public DDLOperations getDDLOperations(){
        if(operations==null)
            operations = createDDLOpt(this.sqlSession.getConnection());
        return operations;
    }



    @Override
    public void createSequence(final String sequenceName) throws SQLException {
        getDDLOperations().createSequence(sequenceName);
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
    public void modifyColumn(final String tableCode,final TableField oldColumn, final TableField column) throws SQLException {
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
        return getDDLOperations().makeCreateSequenceSql(sequenceName);
    }

    @Override
    public String makeCreateTableSql(TableInfo tableInfo) {
        return getDDLOperations().makeCreateTableSql(tableInfo);
    }

    @Override
    public String makeDropTableSql(String tableCode) {
        return getDDLOperations().makeDropTableSql(tableCode);
    }

    @Override
    public String makeAddColumnSql(String tableCode, TableField column) {
        return getDDLOperations().makeAddColumnSql(tableCode,column);
    }

    @Override
    public String makeModifyColumnSql(String tableCode,TableField oldColumn, TableField column) {
        return getDDLOperations().makeModifyColumnSql(tableCode, oldColumn, column);
    }

    @Override
    public String makeDropColumnSql(String tableCode, String columnCode) {
        return getDDLOperations().makeDropColumnSql(tableCode,columnCode);
    }

    @Override
    public String makeRenameColumnSql(String tableCode, String columnCode, TableField column) {
        return getDDLOperations().makeRenameColumnSql(tableCode,columnCode,column);
    }

    @Override
    public List<String> makeReconfigurationColumnSqls(String tableCode, String columnCode, TableField column) {
        return getDDLOperations().makeReconfigurationColumnSqls(tableCode,columnCode,column);
    }
}
