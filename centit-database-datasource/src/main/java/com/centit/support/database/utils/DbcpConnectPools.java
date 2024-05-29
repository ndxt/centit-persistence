package com.centit.support.database.utils;

import com.centit.support.database.metadata.IDatabaseInfo;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public abstract class DbcpConnectPools {
    private static final Logger logger = LoggerFactory.getLogger(DbcpConnectPools.class);
    private static final
    ConcurrentHashMap<DataSourceDescription, HikariDataSource> dbcpDataSourcePools
        = new ConcurrentHashMap<>();
    private DbcpConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static HikariDataSource mapDataSource(DataSourceDescription dsDesc) {
        HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName(dsDesc.getDriver());
        ds.setUsername(dsDesc.getUsername());
        ds.setPassword(dsDesc.getPassword());
        ds.setJdbcUrl(dsDesc.getConnUrl());
        ds.setConnectionTimeout(dsDesc.getMaxWaitMillis());
        ds.setMaximumPoolSize(dsDesc.getMaxTotal());
        ds.setMaxLifetime(180000);
        ds.setIdleTimeout(6000);
        ds.setValidationTimeout(5000);
        ds.setMinimumIdle(dsDesc.getMinIdle());

        String validationQuery = DBType.getDBValidationQuery(dsDesc.getDbType());
        if(StringUtils.isNotBlank(validationQuery)) {
            ds.setConnectionTestQuery(validationQuery);
        }
        return ds;
    }


    public static synchronized HikariDataSource getDataSource(DataSourceDescription dsDesc) {
        HikariDataSource ds = dbcpDataSourcePools.get(dsDesc);
        if (ds == null) {
            ds = mapDataSource(dsDesc);
            dbcpDataSourcePools.put(dsDesc, ds);
        }
        return ds;
    }

    public static synchronized Connection getDbcpConnect(DataSourceDescription dsDesc) throws SQLException {
        HikariDataSource ds = getDataSource(dsDesc);
        Connection conn = ds.getConnection();
        conn.setAutoCommit(false);
        ///*dsDesc.getUsername(),dsDesc.getDbType(),*/
        return conn;
    }

    public static HikariDataSource getDataSource(IDatabaseInfo dbinfo) {
        return DbcpConnectPools.getDataSource(DataSourceDescription.valueOf(dbinfo));
    }

    public static Connection getDbcpConnect(IDatabaseInfo dbinfo) throws SQLException {
        return DbcpConnectPools.getDbcpConnect(DataSourceDescription.valueOf(dbinfo));//.getConn();
    }

    /* 获得数据源连接状态 */
    public static Map<String, Object> getDataSourceStats(DataSourceDescription dsDesc) {
        HikariDataSource bds = dbcpDataSourcePools.get(dsDesc);
        if (bds == null) {
            return null;
        }
        Map<String, Object> map = new HashMap<>(2);
        map.put("poolName", bds.getPoolName());
        return map;
    }

    /**
     * 关闭数据源
     */
    public static synchronized void shutdownDataSource() {
        for (Map.Entry<DataSourceDescription, HikariDataSource> dbs : dbcpDataSourcePools.entrySet()) {
            dbs.getValue().close();
        }
        //dbcpDataSourcePools.clear();
    }

    public static synchronized boolean testDataSource(DataSourceDescription dsDesc) {
        HikariDataSource ds = mapDataSource(dsDesc);
        boolean connOk = false;
        try {
            //Class.forName(dsDesc.getDriver());
            //Connection conn = DriverManager.getConnection(dsDesc.getConnUrl(),
            //dsDesc.getUsername(),dsDesc.getPassword());
            Connection conn = ds.getConnection();
            if (conn != null) {
                connOk = true;
                conn.close();
            }
            ds.close();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
        } finally {
            ds.close();
        }
        return connOk;
    }

    public static void closeConnect(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);//e.printStackTrace();
            }
        }
    }
}
