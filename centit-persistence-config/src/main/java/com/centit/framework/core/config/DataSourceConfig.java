package com.centit.framework.core.config;

import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.framework.flyway.plugin.FlywayExt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringRegularOpt;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.QueryLogUtils;
import com.centit.support.security.AESSecurityUtils;
import org.apache.commons.dbcp2.BasicDataSource;
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

    public static String encryptProperty(String propertyValue) {
        if (StringUtils.isNotBlank(propertyValue)) {
            if (propertyValue.startsWith("cipher:")) {
                return AESSecurityUtils.decryptBase64String(
                    propertyValue.substring(7), AESSecurityUtils.AES_DEFAULT_KEY);
            }
        }
        return propertyValue;
    }

    @Bean(destroyMethod = "close")
    public BasicDataSource dataSource() {

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(env.getProperty("jdbc.driver"));
        dataSource.setUrl(env.getProperty("jdbc.url"));
        dataSource.setUsername(
            AESSecurityUtils.decryptParameterString(encryptProperty(env.getProperty("jdbc.user"))));
        dataSource.setPassword(
            AESSecurityUtils.decryptParameterString(encryptProperty(env.getProperty("jdbc.password"))));
        dataSource.setMaxTotal(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxActive"),100));
        dataSource.setMaxIdle(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxIdle"),50));
        dataSource.setMaxWaitMillis(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.maxWait"), 2000));
        dataSource.setMinIdle(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.minIdle"), 5));
        dataSource.setInitialSize(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.minIdle"), 10));
        dataSource.setDefaultAutoCommit(env.getProperty("jdbc.defaultAutoCommit", Boolean.class));
        dataSource.setRemoveAbandonedOnMaintenance(true);
        dataSource.setRemoveAbandonedOnBorrow(true);
        dataSource.setRemoveAbandonedTimeout(NumberBaseOpt.castObjectToInteger(env.getProperty("jdbc.removeAbandonedTimeout"), 60));
        String validationQuery = env.getProperty("jdbc.validationQuery");
        dataSource.setValidationQuery(validationQuery);
        if ( StringRegularOpt.isTrue(env.getProperty("jdbc.testWhileIdle")) && StringUtils.isNotBlank(validationQuery)){
            dataSource.setTestWhileIdle(true);
        }
        if (StringRegularOpt.isTrue(env.getProperty("jdbc.show.sql"))) {
            QueryLogUtils.setJdbcShowSql(true);
        }

        DBType dbType = DBType.mapDBType(env.getProperty("jdbc.url"));
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

        return dataSource;
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
