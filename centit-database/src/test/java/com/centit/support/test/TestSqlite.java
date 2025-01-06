package com.centit.support.test;

import com.alibaba.fastjson2.JSONObject;
import com.centit.support.algorithm.DatetimeOpt;
import com.centit.support.database.ddl.SqliteDDLOperations;
import com.centit.support.database.jsonmaptable.SqliteJsonObjectDao;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.DatabaseAccess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestSqlite {
    public static void main(String[] args) {

        Connection connection = null;
        // 连接到SQLite数据库（如果数据库不存在，会自动创建）
        try {
            Class.forName(DBType.getDbDriver(DBType.Sqlite));// 这个好像不是必须的
            connection = DriverManager.getConnection("jdbc:sqlite:/Users/codefan/projects/RunData/temp/testInc.db");
            JSONObject object = new JSONObject();
            object.put("id", 137);
            object.put("userName", "codefan");
            object.put("createTime", DatetimeOpt.currentUtilDate());
            SimpleTableInfo tableInfo = SqliteDDLOperations.mapTableInfo(object, "user_info");
            tableInfo.setColumnAsPrimaryKey("id");
            SqliteDDLOperations operations = new SqliteDDLOperations();
            DatabaseAccess.doExecuteSql(connection, operations.makeCreateTableSql(tableInfo));
            SqliteJsonObjectDao jsonObjectDao = new SqliteJsonObjectDao(connection, tableInfo);
            jsonObjectDao.saveNewObject(object);
            object.put("id", 1375);
            jsonObjectDao.saveNewObject(object);
            object.put("id", 135);
            object.put("userName", "wilbur");
            jsonObjectDao.saveNewObject(object);
            object.put("id", 13);
            jsonObjectDao.saveNewObject(object);

            tableInfo = SqliteDDLOperations.mapTableInfo(object, "user_info2");
            tableInfo.setColumnAsPrimaryKey("userName");
            DatabaseAccess.doExecuteSql(connection, operations.makeCreateTableSql(tableInfo));
            jsonObjectDao = new SqliteJsonObjectDao(connection, tableInfo);
            jsonObjectDao.saveNewObject(object);

            /*JdbcMetadata metadata = new JdbcMetadata();
            metadata.setDBConfig(connection);
            List<SimpleTableInfo> tableInfos = metadata.listTables(true, null);
            System.out.println(JSON.toJSONString(tableInfos));*/
            //tableInfos.get(0).setColumnAsPrimaryKey();
            connection.close();
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
