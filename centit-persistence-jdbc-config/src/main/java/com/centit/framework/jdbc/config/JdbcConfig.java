package com.centit.framework.jdbc.config;

import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringRegularOpt;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.QueryLogUtils;
import com.centit.support.security.SecurityOptUtils;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.IOException;

@EnableTransactionManagement(proxyTargetClass = true)//启用注解事物管理
public class JdbcConfig implements EnvironmentAware {

    protected Logger logger = LoggerFactory.getLogger(JdbcConfig.class);
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
        HikariDataSource ds = new HikariDataSource();
        //失败时是否进行重试连接    true:不进行重试   false：进行重试    设置为false时达蒙数据库会出现问题（会导致达蒙连接撑爆挂掉）
        ds.setDriverClassName(env.getProperty("jdbc.driver")); //DBType.getDbDriver(dbType)
        ds.setUsername(SecurityOptUtils.decodeSecurityString(env.getProperty("jdbc.user")));
        ds.setPassword(SecurityOptUtils.decodeSecurityString(env.getProperty("jdbc.password")));
        ds.setJdbcUrl(env.getProperty("jdbc.url"));
        ds.setMaxLifetime(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxLifeTime"), 18000));
        ds.setMaximumPoolSize(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxActive"),100));
        ds.setConnectionTimeout(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxWait"), 5000));
        ds.setMinimumIdle(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.minIdle"), 5));
        ds.setValidationTimeout(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.validationTimeout"), 5000));

        DBType dbType = DBType.mapDBType(env.getProperty("jdbc.url"));
        String validationQuery = env.getProperty("jdbc.validationQuery");

        boolean testWhileIdle = BooleanBaseOpt.castObjectToBoolean(
            env.getProperty("jdbc.testWhileIdle"),true);
        if(StringUtils.isBlank(validationQuery)){
            validationQuery = DBType.getDBValidationQuery(dbType);
        }

        if (testWhileIdle && StringUtils.isNotBlank(validationQuery)){
            ds.setConnectionTestQuery(validationQuery);
        }
        if (StringRegularOpt.isTrue(env.getProperty("jdbc.show.sql"))) {
            QueryLogUtils.setJdbcShowSql(true);
        }

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

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Lazy
    @Bean
    //@DependsOn("flyway")
    public DataSourceTransactionManager transactionManager(@Autowired DataSource dataSource) {
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager();
        transactionManager.setDataSource(dataSource);
        return transactionManager;
    }

    @Lazy
    @Bean
    public PersistenceExceptionTranslationPostProcessor persistenceExceptionTranslationPostProcessor() {
        return new PersistenceExceptionTranslationPostProcessor();
    }

}
