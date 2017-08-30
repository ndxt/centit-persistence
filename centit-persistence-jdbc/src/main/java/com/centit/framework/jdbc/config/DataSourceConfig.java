package com.centit.framework.jdbc.config;

import com.centit.support.algorithm.StringRegularOpt;
import org.apache.commons.dbcp2.BasicDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.context.annotation.*;
import java.util.Properties;

//@Configuration
@EnableTransactionManagement(proxyTargetClass = true)//启用注解事物管理
//@PropertySource("classpath:/system.properties")
//@EnableAspectJAutoProxy(proxyTargetClass = true)
@Lazy
public class DataSourceConfig implements EnvironmentAware {

//    @Autowired
    Environment env;

    @Override
    public void setEnvironment(final Environment environment) {
        this.env = environment;
    }

    @Bean(destroyMethod = "close")
    public BasicDataSource dataSource() {
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
    @Lazy(value = false)
    public Flyway flyway(BasicDataSource dataSource) {
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


    @Bean
    @DependsOn("flyway")
    public DataSourceTransactionManager transactionManager(BasicDataSource dataSource) {
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager();
        transactionManager.setDataSource(dataSource);
        return transactionManager;
    }


    @Bean
    public PersistenceExceptionTranslationPostProcessor persistenceExceptionTranslationPostProcessor() {
        return new PersistenceExceptionTranslationPostProcessor();
    }

    @Bean
    public AutowiredAnnotationBeanPostProcessor autowiredAnnotationBeanPostProcessor() {
        return new AutowiredAnnotationBeanPostProcessor();
    }



}
