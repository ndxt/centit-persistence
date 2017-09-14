package com.centit.framework.hibernate.config;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.orm.hibernate5.support.OpenSessionInViewFilter;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.servlet.ServletContext;
import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@EnableTransactionManagement(proxyTargetClass = true)//启用注解事物管理
@EnableAspectJAutoProxy(proxyTargetClass = true)
@Lazy
public class HibernateConfig implements EnvironmentAware {

    protected Environment env;

    @Override
    public void setEnvironment(Environment environment) {
        this.env = environment;
    }

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Bean
    public LocalSessionFactoryBean sessionFactory(@Autowired DataSource dataSource) {
        LocalSessionFactoryBean sessionFactory = new LocalSessionFactoryBean();
        sessionFactory.setDataSource(dataSource);
        Properties hibernateProperties = new Properties();
        hibernateProperties.put("hibernate.dialect", env.getProperty("jdbc.dialect"));
        hibernateProperties.put("hibernate.show_sql",
                env.getProperty("jdbc.show.sql")==null || Boolean.parseBoolean(env.getProperty("jdbc.show.sql")));
        hibernateProperties.put("hibernate.id.new_generator_mappings", true);
        hibernateProperties.put("hibernate.connection.release_mode","after_statement");
        sessionFactory.setHibernateProperties(hibernateProperties);
        String[] packagesToScan = new String[]{"com.centit.*"};
        sessionFactory.setPackagesToScan(packagesToScan);
        return  sessionFactory;
    }

    @Bean
    @DependsOn("flyway")
    public HibernateTransactionManager transactionManager(@Autowired SessionFactory sessionFactory) {
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

    /**
     * 注册OpenSessionInViewFilter 过滤器
     * @param servletContext ServletContext
     */
    public static void registerOpenSessionInViewFilter(ServletContext servletContext) {
        javax.servlet.FilterRegistration.Dynamic openSessionInViewFilter
                = servletContext.addFilter("openSessionInViewFilter",
                OpenSessionInViewFilter.class);
        openSessionInViewFilter.setAsyncSupported(true);
        openSessionInViewFilter.setInitParameter("flushMode", "AUTO");
        openSessionInViewFilter.setInitParameter("singleSession", "true");
        openSessionInViewFilter.addMappingForUrlPatterns(null, false, "*.jsp", "/system/*", "/service/*");
    }
}
