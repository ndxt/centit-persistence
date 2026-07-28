package com.centit.support.database.utils;

import com.centit.support.database.metadata.IDatabaseInfo;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
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

    /** connectionTimeout 保护上限：Hikari 的 borrow 循环在 elapsed >= connectionTimeout 时才抛超时，
     *  若被配成几十万毫秒（常见于把秒当毫秒），单次获取连接即可把业务线程挂死十几分钟。
     *  超过此值视为配置错误，夹紧到上限并告警。 */
    private static final int MAX_CONNECTION_TIMEOUT_MS = 60000;

    /** socketTimeout 默认值(ms)：防止连接校验在僵尸 TCP 上 hang 到 OS 层 TCP 重传超时（Linux 默认约 940 秒）。 */
    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 30000;
    /** connectTimeout 默认值(ms) */
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 5000;
    /** maxLifetime 默认 30 分钟(对齐 Hikari 官方默认)，过短(如 3 分钟)会导致连接频繁重建。
     *  注意应小于数据库 wait_timeout。 */
    private static final int DEFAULT_MAX_LIFETIME_MS = 1800000;
    /** idleTimeout 默认 10 分钟 */
    private static final int DEFAULT_IDLE_TIMEOUT_MS = 600000;
    /** validationTimeout 默认 5 秒 */
    private static final int DEFAULT_VALIDATION_TIMEOUT_MS = 5000;
    /** 泄漏检测阈值默认 60 秒：连接借出超过该阈值未归还，Hikari 会打印借出调用栈，便于定位泄漏。 */
    private static final int DEFAULT_LEAK_DETECTION_THRESHOLD_MS = 60000;
    /** 连接获取慢日志阈值(ms) */
    private static final long SLOW_CONNECT_THRESHOLD_MS = 200;

    /** 以稳定的 databaseCode 作为池键(为空时回退到 connUrl|username)。
     *  DataSourceDescription.equals/hashCode 仅基于 connUrl/username，若以整个对象为键，
     *  配置变更(password 等)后新对象与旧键不相等，refreshDataSource/delDataSource 会因 containsKey 不命中
     *  而跳过关闭，导致旧池永久滞留。改用稳定键后，同一数据源始终命中同一个池。 */
    private static final
    ConcurrentHashMap<String, HikariDataSource> dbcpDataSourcePools
        = new ConcurrentHashMap<>();

    private DbcpConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    /**
     * 解析连接池键：优先使用 databaseCode，为空时回退到 connUrl|username 组合键
     * （与原 DataSourceDescription.equals/hashCode 所用字段一致，保证旧用法兼容）。
     */
    private static String resolvePoolKey(DataSourceDescription dsDesc) {
        String code = dsDesc.getDatabaseCode();
        if (StringUtils.isNotBlank(code)) {
            return code;
        }
        return (dsDesc.getConnUrl() == null ? "" : dsDesc.getConnUrl()) + "|"
            + (dsDesc.getUsername() == null ? "" : dsDesc.getUsername());
    }

    private static HikariDataSource createDataSource(DataSourceDescription dsDesc) {
        HikariDataSource ds = new HikariDataSource();
        String poolKey = resolvePoolKey(dsDesc);
        // poolName 便于日志/监控/JMX 定位到具体数据源
        ds.setPoolName(poolKey);

        DBType dbType = dsDesc.getDbType();
        ds.setDriverClassName(dsDesc.getDriver());
        ds.setUsername(dsDesc.getUsername());
        ds.setPassword(dsDesc.getPassword());

        int socketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
        int connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
        ds.setJdbcUrl(buildJdbcUrlWithTimeout(dsDesc.getConnUrl(), dbType, socketTimeoutMs, connectTimeoutMs));
        if (dbType == DBType.Oracle || dbType == DBType.DM || dbType == DBType.Oscar) {
            ds.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", String.valueOf(connectTimeoutMs));
            ds.addDataSourceProperty("oracle.jdbc.ReadTimeout", String.valueOf(socketTimeoutMs));
        }

        ds.setMaximumPoolSize(dsDesc.getMaxTotal());
        ds.setMaxLifetime(DEFAULT_MAX_LIFETIME_MS);
        ds.setIdleTimeout(DEFAULT_IDLE_TIMEOUT_MS);

        ds.setConnectionTimeout(resolveConnectionTimeout(dsDesc));

        ds.setMinimumIdle(dsDesc.getMinIdle());

        // 不再按 dbType 自动填默认 validationQuery：HikariCP 在未设置 connectionTestQuery 时
        // 会用 JDBC4 Connection.isValid(validationTimeout) 校验，这是官方推荐路径；且 isValid 通常
        // 能正确遵守 validationTimeout，避免在僵尸连接上靠 Statement.setQueryTimeout 限时被驱动忽略而挂死。
        ds.setValidationTimeout(DEFAULT_VALIDATION_TIMEOUT_MS);

        // 泄漏检测：连接借出超过该阈值未归还，Hikari 会打印借出调用栈，便于定位泄漏。
        // 设为 0 可关闭；须 < maxLifetime 且 >= 2000ms。
        ds.setLeakDetectionThreshold(DEFAULT_LEAK_DETECTION_THRESHOLD_MS);

        return ds;
    }

    /**
     * 解析 connectionTimeout，并对异常大的值做夹紧保护。
     */
    private static int resolveConnectionTimeout(DataSourceDescription dsDesc) {
        int configured = dsDesc.getMaxWaitMillis();
        if (configured > MAX_CONNECTION_TIMEOUT_MS) {
            logger.warn("数据源 [{}] 的 maxWaitMillis(connectionTimeout) 配置为 {}ms，超过保护上限 {}ms，已夹紧为上限。" +
                    "过大的 connectionTimeout 会让单次获取连接挂住线程数分钟，请检查是否把秒当成了毫秒。",
                resolvePoolKey(dsDesc), configured, MAX_CONNECTION_TIMEOUT_MS);
            return MAX_CONNECTION_TIMEOUT_MS;
        }
        return configured;
    }

    /**
     * 根据 DBType 为 JDBC URL 追加 socket/connect 超时参数，防止连接校验在僵尸 TCP 上 hang 到
     * OS 层 TCP 重传超时（Linux 默认约 940 秒）。
     * <p>
     * MySQL/PostgreSQL/KingBase/SQLServer 通过 URL 参数设置；
     * Oracle/DM/Oscar 不支持 URL 参数，返回原始 URL，由调用方通过 addDataSourceProperty 设置。
     */
    private static String buildJdbcUrlWithTimeout(String jdbcUrl, DBType dbType,
                                                   int socketTimeoutMs, int connectTimeoutMs) {
        if (jdbcUrl == null || dbType == null) {
            return jdbcUrl;
        }
        switch (dbType) {
            case MySql:
                return appendUrlParam(jdbcUrl, false,
                    "connectTimeout", String.valueOf(connectTimeoutMs),
                    "socketTimeout", String.valueOf(socketTimeoutMs));
            case PostgreSql:
            case KingBase:
                // PostgreSQL / KingBase 超时单位为秒
                return appendUrlParam(jdbcUrl, false,
                    "connectTimeout", String.valueOf(connectTimeoutMs / 1000),
                    "socketTimeout", String.valueOf(socketTimeoutMs / 1000));
            case SqlServer:
                // SQL Server 用分号分隔，超时单位为秒
                return appendUrlParam(jdbcUrl, true,
                    "loginTimeout", String.valueOf(connectTimeoutMs / 1000),
                    "socketTimeout", String.valueOf(socketTimeoutMs / 1000));
            default:
                return jdbcUrl;
        }
    }

    /**
     * 为 JDBC URL 追加键值对参数，自动处理分隔符并跳过已存在的同名参数。
     *
     * @param url                 原始 JDBC URL
     * @param semicolonSeparated  true=分号分隔（SQL Server），false=问号/与号分隔（MySQL/PG）
     * @param kvPairs             key1, value1, key2, value2, ...
     */
    private static String appendUrlParam(String url, boolean semicolonSeparated, String... kvPairs) {
        StringBuilder sb = new StringBuilder(url);
        boolean hasQuery = url.contains("?");
        for (int i = 0; i < kvPairs.length; i += 2) {
            String key = kvPairs[i];
            String value = kvPairs[i + 1];
            if (url.contains(key + "=")) {
                continue;
            }
            if (semicolonSeparated) {
                sb.append(";").append(key).append("=").append(value);
            } else {
                sb.append(hasQuery ? "&" : "?").append(key).append("=").append(value);
                hasQuery = true;
            }
        }
        return sb.toString();
    }

    /**
     * 刷新数据源连接池：按池键原子替换旧池并关闭。
     * 不再以整个 DataSourceDescription 判断 containsKey——其多字段 equals 在配置变更后会不命中而漏关旧池。
     */
    public static synchronized void refreshDataSource(DataSourceDescription dsDesc) {
        String poolKey = resolvePoolKey(dsDesc);
        if (dbcpDataSourcePools.containsKey(poolKey)) {
            HikariDataSource newDs = createDataSource(dsDesc);
            HikariDataSource oldDs = dbcpDataSourcePools.put(poolKey, newDs);
            closeQuietly(oldDs, poolKey);
        }
    }

    /**
     * 删除数据源连接池：按池键原子移除并关闭。
     * 使用 remove 原子返回旧值，避免 get/remove 两步间的竞态。
     */
    public static synchronized void delDataSource(DataSourceDescription dsDesc) {
        String poolKey = resolvePoolKey(dsDesc);
        HikariDataSource oldDs = dbcpDataSourcePools.remove(poolKey);
        closeQuietly(oldDs, poolKey);
    }

    public static HikariDataSource getDataSource(DataSourceDescription dsDesc) {
        String poolKey = resolvePoolKey(dsDesc);
        // computeIfAbsent 保证池的懒创建线程安全，无需 synchronized 全局锁。
        // 之前的 synchronized 会让所有数据源、所有线程串行获取连接，一旦某个线程因池耗尽而
        // 阻塞（最长 connectionTimeout），其余线程全部被锁死，引发雪崩式超时。
        return dbcpDataSourcePools.computeIfAbsent(poolKey, k -> createDataSource(dsDesc));
    }

    public static Connection getDbcpConnect(DataSourceDescription dsDesc) throws SQLException {
        String poolKey = resolvePoolKey(dsDesc);
        HikariDataSource ds = getDataSource(dsDesc);
        long start = System.currentTimeMillis();
        try {
            Connection conn = ds.getConnection();
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > SLOW_CONNECT_THRESHOLD_MS) {
                HikariPoolMXBean mx = ds.getHikariPoolMXBean();
                logger.warn("获取连接较慢 [{}]: {}ms, active={}, idle={}, total={}",
                    poolKey, elapsed,
                    mx.getActiveConnections(), mx.getIdleConnections(), mx.getTotalConnections());
            }
            try {
                conn.setAutoCommit(false);
            } catch (SQLException sqle) {
                // setAutoCommit 失败必须立即归还连接，否则池将连接计为 active 直到外部超时回收
                closeConnect(conn);
                throw sqle;
            }
            return conn;
        } catch (SQLException e) {
            // 池耗尽或 autoCommit 失败时快速失败并打印池状态，便于定位是连接泄漏还是慢查询。
            logPoolStatus(poolKey, ds, e);
            throw e;
        }
    }

    /**
     * 打印 HikariCP 池状态，用于连接获取失败时的诊断。
     */
    private static void logPoolStatus(String poolKey, HikariDataSource ds, SQLException e) {
        try {
            if (ds.isClosed()) {
                logger.error("获取数据库连接失败，数据源 [{}] 的连接池已关闭: {}",
                    poolKey, e.getMessage(), e);
                return;
            }
            // getHikariPoolMXBean 在池启动后才可用
            HikariPoolMXBean mxBean = ds.getHikariPoolMXBean();
            if (mxBean != null) {
                logger.error("获取数据库连接失败，数据源 [{}] 池状态: active={}, idle={}, 等待线程数={}, 总连接={}, max={}, 错误: {}",
                    poolKey,
                    mxBean.getActiveConnections(), mxBean.getIdleConnections(),
                    mxBean.getThreadsAwaitingConnection(), mxBean.getTotalConnections(),
                    ds.getMaximumPoolSize(), e.getMessage(), e);
            } else {
                logger.error("获取数据库连接失败，数据源 [{}]: {}", poolKey, e.getMessage(), e);
            }
        } catch (Exception ignore) {
            logger.error(e.getMessage(), e);
        }
    }

    public static HikariDataSource getDataSource(IDatabaseInfo dbinfo) {
        return DbcpConnectPools.getDataSource(DataSourceDescription.valueOf(dbinfo));
    }

    public static Connection getDbcpConnect(IDatabaseInfo dbinfo) throws SQLException {
        return DbcpConnectPools.getDbcpConnect(DataSourceDescription.valueOf(dbinfo));
    }

    /* 获得数据源连接状态 */
    public static Map<String, Object> getDataSourceStats(DataSourceDescription dsDesc) {
        String poolKey = resolvePoolKey(dsDesc);
        HikariDataSource bds = dbcpDataSourcePools.get(poolKey);
        if (bds == null) {
            return null;
        }
        Map<String, Object> map = new HashMap<>(8);
        map.put("poolName", bds.getPoolName());
        try {
            HikariPoolMXBean mx = bds.getHikariPoolMXBean();
            if (mx != null) {
                map.put("activeConnections", mx.getActiveConnections());
                map.put("idleConnections", mx.getIdleConnections());
                map.put("totalConnections", mx.getTotalConnections());
                map.put("threadsAwaitingConnection", mx.getThreadsAwaitingConnection());
            }
        } catch (Exception ignore) {
            // 忽略池未启动等异常
        }
        return map;
    }

    /**
     * 关闭数据源（等价于 closeAllDataSources，保留以兼容旧 API）。
     */
    public static synchronized void shutdownDataSource() {
        closeAllDataSources();
    }

    /**
     * 关闭并清空全部连接池。供应用关闭(@PreDestroy)调用，避免热部署/上下文重启时
     * 遗留 Hikari housekeeper 线程与数据库会话。逐池容错，一个失败不阻断其余。
     */
    public static synchronized void closeAllDataSources() {
        int count = dbcpDataSourcePools.size();
        for (Map.Entry<String, HikariDataSource> entry : dbcpDataSourcePools.entrySet()) {
            closeQuietly(entry.getValue(), entry.getKey());
        }
        dbcpDataSourcePools.clear();
        logger.info("已关闭全部数据库连接池，共 {} 个", count);
    }

    /**
     * 关闭单个连接池，吞掉异常以便逐池清理时一个失败不阻断其余。
     */
    private static void closeQuietly(HikariDataSource ds, String poolKey) {
        if (ds == null) {
            return;
        }
        try {
            ds.close();
        } catch (Exception e) {
            logger.error("关闭数据源 [{}] 连接池失败: {}", poolKey, e.getMessage(), e);
        }
    }

    public static boolean testDataSource(DataSourceDescription dsDesc) {
        // 测试能否成功获取连接：获取成功即视为连通性正常，由 try-with-resources 自动关闭
        try (HikariDataSource ds = createDataSource(dsDesc);
             Connection conn = ds.getConnection()) {
            return true;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    public static void closeConnect(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
