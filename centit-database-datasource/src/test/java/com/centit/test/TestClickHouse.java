package com.centit.test;

import com.alibaba.fastjson2.JSONArray;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.DbcpConnectPools;

import java.sql.Connection;

public class TestClickHouse {
    public static void main(String[] args) {
        DataSourceDescription dbc = new DataSourceDescription();
        dbc.setConnUrl("jdbc:clickhouse://192.168.134.250:31001/default");
        dbc.setUsername("admin");
        dbc.setPassword("2s4ATz2M");
        try {
            Connection conn = DbcpConnectPools.getDbcpConnect(dbc);
            GeneralJsonObjectDao jsonObjectDao = GeneralJsonObjectDao.createJsonObjectDao(DBType.ClickHouse, conn);
            JSONArray jsonArray = jsonObjectDao.findObjectsAsJSON("select * from t_order_mt", null, null);
            System.out.println(jsonArray.toJSONString());
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("done!");
    }
}
