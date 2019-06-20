package com.centit.framework.core.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * 引用
 * https://github.com/bingzhilanmo-bowen/spring-multi-datasource/blob/master
 * /datasources-base/src/main/java/com/bingzhilanmo/base/datasource/DynamicDataSource.java
 *
 */
public class DynamicDataSource extends AbstractRoutingDataSource {
    final static Logger logger = LoggerFactory.getLogger(DynamicDataSource.class);

    @Override
    protected Object determineCurrentLookupKey(){
        return DynamicDataSourceContextHolder.getDataSourceType();
    }
}
