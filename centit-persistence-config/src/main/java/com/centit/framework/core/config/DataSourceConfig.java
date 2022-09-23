package com.centit.framework.core.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.framework.flyway.plugin.FlywayExt;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringRegularOpt;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.QueryLogUtils;
import com.centit.support.security.AESSecurityUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

public class DataSourceConfig implements EnvironmentAware {
    protected Logger logger = LoggerFactory.getLogger(DataSourceConfig.class);
    protected Environment env;

    @Resource
    @Override
    public void setEnvironment(Environment environment) {
        if (environment != null) {
            this.env = environment;
        }
    }

    @Bean(destroyMethod = "close")
    public DataSource dataSource() {
        DruidDataSource ds = new DruidDataSource();
        //失败时是否进行重试连接    true:不进行重试   false：进行重试    设置为false时达蒙数据库会出现问题（会导致达蒙连接撑爆挂掉）
        ds.setDriverClassName(env.getProperty("jdbc.driver")); //DBType.getDbDriver(dbType)
        ds.setUsername(AESSecurityUtils.decryptParameterString(env.getProperty("jdbc.user")));
        ds.setPassword(AESSecurityUtils.decryptParameterString(env.getProperty("jdbc.password")));
        ds.setUrl(env.getProperty("jdbc.url"));
        ds.setInitialSize(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.initSize"), 5));
        ds.setMaxActive(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxActive"),100));
        ds.setMaxWait(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxWait"), 10000));
        ds.setMinIdle(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.minIdle"), 5));

        DBType dbType = DBType.mapDBType(env.getProperty("jdbc.url"));
        String validationQuery = env.getProperty("jdbc.validationQuery");

        boolean testWhileIdle = BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.testWhileIdle"),true);
        if(StringUtils.isBlank(validationQuery)){
            validationQuery = DBType.getDBValidationQuery(dbType);
        }

        if (testWhileIdle && StringUtils.isNotBlank(validationQuery)){
            ds.setValidationQuery(validationQuery);
            ds.setTestWhileIdle(true);
        }
        if (StringRegularOpt.isTrue(env.getProperty("jdbc.show.sql"))) {
            QueryLogUtils.setJdbcShowSql(true);
        }

        ds.setBreakAfterAcquireFailure(BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.breakAfterAcquireFailure"),false));
        ds.setTimeBetweenConnectErrorMillis(NumberBaseOpt.castObjectToInteger(
            env.getProperty("jdbc.timeBetweenConnectErrorMillis"), 6000));
        ds.setConnectionErrorRetryAttempts(NumberBaseOpt.castObjectToInteger(
            env.getProperty("jdbc.connectionErrorRetryAttempts"), 1));
        ds.setTestWhileIdle(BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.testWhileIdle"), true));
        ds.setValidationQueryTimeout(NumberBaseOpt.castObjectToInteger(
            env.getProperty("jdbc.validationQueryTimeout"), 1000 * 10));
        ds.setKeepAlive(BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.keepAlive"), true));
        ds.setTimeBetweenEvictionRunsMillis(NumberBaseOpt.castObjectToInteger(
            env.getProperty("jdbc.timeBetweenEvictionRunsMillis"), 60000));
        ds.setMinEvictableIdleTimeMillis(NumberBaseOpt.castObjectToInteger(
            env.getProperty("jdbc.minEvictableIdleTimeMillis"), 300000));
        ds.setRemoveAbandoned(BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.removeAbandoned"), true));
        ds.setRemoveAbandonedTimeout(NumberBaseOpt.castObjectToInteger(
            env.getProperty("jdbc.removeAbandonedTimeout"), 80));
        ds.setLogAbandoned(BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.logAbandoned"), true));
        ds.setTestOnBorrow(BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.testOnBorrow"), true));
        ds.setTestOnReturn(BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.testOnReturn"), false));
        try {
            ExtendedQueryPool.loadResourceExtendedSqlMap(dbType);
        } catch (DocumentException e) {
            logger.error(e.getMessage());
        }
        try {
            ExtendedQueryPool.loadExtendedSqlMaps(
                env.getProperty("app.home", ".") + "/sqlscript", dbType);
        } catch (DocumentException | IOException e) {
            logger.error(e.getMessage());
        }
        return ds;
    }

    @Bean
    public Flyway flyway(DataSource dataSource) throws SQLException {
        String flywayEnable = env.getProperty("flyway.enable");
        if (StringRegularOpt.isTrue(flywayEnable)) {
            Flyway flywayMigration;
            DBType dbType = DBType.mapDBType(dataSource.getConnection());
            if (dbType.isMadeInChina()) {
                flywayMigration = new FlywayExt();
            } else {
                flywayMigration = new Flyway();
            }
            flywayMigration.setDataSource(dataSource);
            flywayMigration.setBaselineOnMigrate(true);
            flywayMigration.setLocations(env.getProperty("flyway.sql.dir").concat(",com.centit.framework.system.update").split(","));
            flywayMigration.migrate();
            return flywayMigration;
        } else {
            return null;
        }
    }

}
