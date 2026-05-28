package com.centit.support.database.metadata;

import com.centit.support.database.utils.DBType;

import java.sql.Connection;
import java.util.List;

@SuppressWarnings("unused")
public interface DatabaseMetadata {

    static DatabaseMetadata createDatabaseMetadata(final DBType dbtype)  {
        return switch (dbtype) {
            case Oracle, DM, KingBase, Oscar -> new OracleMetadata();
            case DB2 -> new DB2Metadata();
            case SqlServer -> new SqlSvrMetadata();
            default -> new JdbcMetadata();
        };
    }

    void setDBConfig(Connection dbc);

    SimpleTableInfo getTableMetadata(String tabName);

    String getDBSchema();

    void setDBSchema(String schema);

    List<SimpleTableInfo> listTables(boolean withColumn, String[] tableNames);

}
