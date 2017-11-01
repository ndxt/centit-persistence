package com.centit.framework.core.config;

import com.centit.support.algorithm.StringRegularOpt;
import org.apache.commons.dbcp2.BasicDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;

public class DataSourceConfig implements EnvironmentAware {

    protected Environment env;

    @Override
    public void setEnvironment(Environment environment) {
        this.env = environment;
    }

    @Bean(destroyMethod = "close")
    public BasicDataSource dataSource() throws PropertyVetoException {
        /*String dataSourcePoolType = env.getProperty("connection.pool.type");
        if("proxool".equals(dataSourcePoolType)) {
            ProxoolDataSource dataSource = new ProxoolDataSource();
            dataSource.setDriver(env.getProperty("jdbc.driver"));
            dataSource.setDriverUrl(env.getProperty("jdbc.url"));
            dataSource.setUser(env.getProperty("jdbc.user"));
            dataSource.setPassword(env.getProperty("jdbc.password"));
            dataSource.setMaximumConnectionCount(env.getProperty("jdbc.maxActive",Integer.class));
            dataSource.setTestBeforeUse(true);
            dataSource.setHouseKeepingSleepTime(600000);//间隔10分钟检查所有连接是否需要关闭或创建
            dataSource.setMaximumActiveTime(600000);//连接超时时间
            return dataSource;

        }else if("c3p0".equals(dataSourcePoolType)) {
            ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass(env.getProperty("jdbc.driver"));
            dataSource.setJdbcUrl(env.getProperty("jdbc.url"));
            dataSource.setUser(env.getProperty("jdbc.user"));
            dataSource.setPassword(env.getProperty("jdbc.password"));
            dataSource.setInitialPoolSize(env.getProperty("jdbc.maxActive", Integer.class));
            dataSource.setMaxIdleTimeExcessConnections(env.getProperty("jdbc.maxIdle", Integer.class));
            dataSource.setCheckoutTimeout(env.getProperty("jdbc.maxWait", Integer.class));
            dataSource.setAcquireIncrement(Integer.parseInt(env.getProperty("jdbc.acquireIncrement")));
            dataSource.setAcquireRetryAttempts(Integer.parseInt(env.getProperty("jdbc.acquireRetryAttempts")));
            dataSource.setAcquireRetryDelay(Integer.parseInt(env.getProperty("jdbc.acquireRetryDelay")));
            dataSource.setIdleConnectionTestPeriod(Integer.parseInt(env.getProperty("jdbc.idleConnectionTestPeriod")));
            dataSource.setPreferredTestQuery(env.getProperty("jdbc.validationQuery"));
            return dataSource;
        }*/
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(env.getProperty("jdbc.driver"));
        dataSource.setUrl(env.getProperty("jdbc.url"));
        dataSource.setUsername(env.getProperty("jdbc.user"));
        dataSource.setPassword(env.getProperty("jdbc.password"));
        dataSource.setMaxTotal(env.getProperty("jdbc.maxActive", Integer.class));
        dataSource.setMaxIdle(env.getProperty("jdbc.maxIdle", Integer.class));
        dataSource.setMaxWaitMillis(env.getProperty("jdbc.maxWait", Integer.class));
        dataSource.setDefaultAutoCommit(env.getProperty("jdbc.defaultAutoCommit", Boolean.class));
        dataSource.setRemoveAbandonedTimeout(env.getProperty("jdbc.removeAbandonedTimeout", Integer.class));
        return dataSource;
    }

    @Bean
    public Flyway flyway(DataSource dataSource) {
        String flywayEnable = env.getProperty("flyway.enable");
        if(StringRegularOpt.isTrue(flywayEnable)){
            Flyway flywayMigration = new Flyway();
            flywayMigration.setDataSource(dataSource);
            flywayMigration.setBaselineOnMigrate(true);
            flywayMigration.setLocations(env.getProperty("flyway.sql.dir"), "com.centit.framework.system.update");
            flywayMigration.migrate();
            return flywayMigration;
        }else{
            return null;
        }
    }

}
