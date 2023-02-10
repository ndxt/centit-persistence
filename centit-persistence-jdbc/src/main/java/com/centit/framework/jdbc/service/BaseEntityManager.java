package com.centit.framework.jdbc.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.utils.PageDesc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
@SuppressWarnings("unused")
public interface BaseEntityManager<T extends Serializable, PK extends Serializable> {

    /**
     * 查找表中的所有记录， 包括禁用的 isValid = 'F' 的记录, 如果没有isValid这个字段也可以使用
     *
     * @return 表中的所有记录， 包括禁用的 isValid = 'F' 的记录, 如果没有isValid这个字段也可以使用
     */
    List<T> listObjects();

    /**
     * 根据过滤条件筛选
     * @param properties 过滤条件
     * @param pageDesc 分页信息
     * @return 过滤后的对象
     */
    @Deprecated
    List<T> listObjectsByProperties(Map<String, Object> properties, PageDesc pageDesc);
    /**
     * 根据属性筛选 严格等于
     * @param propertyName 属性名
     * @param propertyValue 属性值
     * @return 过滤后的对象
     */
    List<T> listObjectsByProperty(String propertyName, Object propertyValue);

    /**
     * 根据属性筛选 严格等于
     * @param filterMap 多个属性组成的map
     * @return 过滤后的对象
     */
    List<T> listObjectsByProperties(Map<String, Object> filterMap);

    /**
     * 根据对象的主键 获得数据库中对应的对象信息
     *
     * @param id PK
     * @return 数据库中对应的对象信息
     */
    T getObjectById(PK id);


    /**
     * 保存泛型参数对象
     *
     * @param o T
     *  Serializable
     */
    void saveNewObject(T o);

    /**
     * 更新泛型参数对象
     *
     * @param o T
     */
    void updateObject(T o);

    /**
     * 保存泛型参数对象
     *
     * @param o T
     */
    void mergeObject(T o);


    /**
     * 删除泛型参数对象
     *
     * @param o T
     */
    void deleteObject(T o);

    /**
     * 根据主键删除泛型参数对象
     *
     * @param id PK
     */
    void deleteObjectById(PK id);


    /**
     * 根据唯一属性值返回对象
     *
     * @param propertyName 字段名
     * @param propertyValue 值
     * @return 唯一属性值返回对象
     */
    T getObjectByProperty(final String propertyName, final Object propertyValue);

    /**
     * 根据多个属性返回唯一对象
     *
     * @param properties map 字段
     * @return 多个属性返回唯一对象
     */
    T getObjectByProperties(Map<String, Object> properties);

    /**
     * 查询数据库，只能查询Po对应的表
     * @param properties 过滤条件
     * @param pageDesc 分页信息
     * @return JSONArray
     */
    JSONArray listObjectsAsJson(Map<String, Object> properties, PageDesc pageDesc);

    /**
     * 查询数据库，可以查询任意表
     * @param querySql  自定义sql语句
     * @param filterMap 过滤条件
     * @param pageDesc 分页信息
     * @return JSONArray
     */
    JSONArray listObjectsBySqlAsJson(String querySql,
                                            Map<String, Object> filterMap,  PageDesc pageDesc );
}
