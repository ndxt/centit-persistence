package com.centit.framework.hibernate.config;

import com.centit.support.algorithm.StringRegularOpt;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.flywaydb.core.Flyway;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.beans.PropertyVetoException;
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

//    @Bean(destroyMethod = "close")
//    public BasicDataSource dataSource() {
//        BasicDataSource dataSource = new BasicDataSource();
//        dataSource.setDriverClassName(env.getProperty("jdbc.driver"));
//        dataSource.setUrl(env.getProperty("jdbc.url"));
//        dataSource.setUsername(env.getProperty("jdbc.user"));
//        dataSource.setPassword(env.getProperty("jdbc.password"));
//        dataSource.setMaxTotal(env.getProperty("jdbc.maxActive", Integer.class));
//        dataSource.setMaxIdle(env.getProperty("jdbc.maxIdle", Integer.class));
//        dataSource.setMaxWaitMillis(env.getProperty("jdbc.maxWait", Integer.class));
//        dataSource.setDefaultAutoCommit(env.getProperty("jdbc.defaultAutoCommit", Boolean.class));
//        dataSource.setRemoveAbandonedTimeout(env.getProperty("jdbc.removeAbandonedTimeout", Integer.class));
//        return dataSource;
//    }

//    <!-- <bean id="dataSource"
//    class="com.mchange.v2.c3p0.ComboPooledDataSource"
//    p:driverClass="${jdbc.driver}"
//    p:jdbcUrl="${jdbc.url}"
//    p:user="${jdbc.user}"
//    p:password="${jdbc.password}"
//    p:initialPoolSize="${jdbc.minSize}"
//    p:minPoolSize="${jdbc.minSize}"
//    p:maxPoolSize="${jdbc.maxActive}"
//    p:maxIdleTimeExcessConnections="${jdbc.maxIdle}"
//    p:checkoutTimeout="${jdbc.maxWait}"

//    p:acquireIncrement="${jdbc.cquireIncrement}"
//    p:acquireRetryAttempts="${jdbc.acquireRetryAttempts}"
//    p:acquireRetryDelay="${jdbc.acquireRetryDelay}"
//    p:idleConnectionTestPeriod="${jdbc.idleConnectionTestPeriod}"
//    p:preferredTestQuery="${jdbc.validationQuery}" />
//            -->
    @Bean(destroyMethod = "close")
    public ComboPooledDataSource dataSource() throws PropertyVetoException {
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
    }

    @Bean
    public LocalSessionFactoryBean sessionFactory(ComboPooledDataSource dataSource) {
        LocalSessionFactoryBean sessionFactory = new LocalSessionFactoryBean();
        sessionFactory.setDataSource(dataSource);
        Properties hibernateProperties = new Properties();
        hibernateProperties.put("hibernate.dialect", env.getProperty("jdbc.dialect"));
        hibernateProperties.put("hibernate.show_sql", Boolean.parseBoolean(env.getProperty("jdbc.show.sql")));
        hibernateProperties.put("hibernate.id.new_generator_mappings", true);
        sessionFactory.setHibernateProperties(hibernateProperties);
        String[] packagesToScan = new String[]{"com.centit.*"};
        sessionFactory.setPackagesToScan(packagesToScan);
        return  sessionFactory;
    }

    @Bean
    @Lazy(value = false)
    public Flyway flyway(ComboPooledDataSource dataSource) {
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
    public HibernateTransactionManager transactionManager(SessionFactory sessionFactory) {
        HibernateTransactionManager transactionManager = new HibernateTransactionManager();
        transactionManager.setSessionFactory(sessionFactory);
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

   /* @Bean
    public ReloadableResourceBundleMessageSource validationMessageSource(){
        ReloadableResourceBundleMessageSource validationMessageSource = new ReloadableResourceBundleMessageSource();
        validationMessageSource.setBasename("classpath:messagesource/validation/validation");
        validationMessageSource.setFallbackToSystemLocale(false);
        validationMessageSource.setUseCodeAsDefaultMessage(false);
        validationMessageSource.setDefaultEncoding("UTF-8");
        validationMessageSource.setCacheSeconds(120);
        return  validationMessageSource;
    }

    @Bean
    public LocalValidatorFactoryBean validator() {
        LocalValidatorFactoryBean localValidatorFactoryBean = new LocalValidatorFactoryBean();
        localValidatorFactoryBean.setProviderClass(HibernateValidator.class);
        localValidatorFactoryBean.setValidationMessageSource(validationMessageSource());
        return  localValidatorFactoryBean;
    }*/

}
