package com.centit.framework.core.po;

import java.util.Date;

/**
 * PO实体实现这个接口，数据中新建和更新时会自动修改这个时间戳，
 * 前提条件是调用Dao的创建、更新、逻辑删除接口，而不是直接调用HQL
 * @author codefan
 * hibernate 中实现了这个接口
 * jdbc 中没有实现这个接口，请用注解
 *      ValueGenerator(strategy=GeneratorType.FUNCTIION, occasion=GeneratorTime.ALWAYS,
 *          condition=GeneratorCondition.ALWAYS, value="now") 来代替
 * @see com.centit.support.database.orm.ValueGenerator
 * mybatis 中没有实现这个接口也没有代替方案
 */
@Deprecated
public interface EntityWithTimestamp{
    /**
     * 获取最后更新时间戳
     * @return 最后更新时间戳
     */
    Date getLastModifyDate();
    /**
     * 设置最后更新时间戳
     * @param lastModifyDate Date
     */
    void setLastModifyDate(Date lastModifyDate);

}
