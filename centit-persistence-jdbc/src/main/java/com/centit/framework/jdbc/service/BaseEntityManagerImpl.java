package com.centit.framework.jdbc.service;

import com.centit.framework.core.dao.PageDesc;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.support.database.utils.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 数据库的基本操作工具类
 * 基本上是对Dao进行再一次简单的封装 注解Manager，添加默认事务
 *
 * @author codefan
 * 2012-2-16
 */
public abstract class BaseEntityManagerImpl<T extends Serializable,
 		PK extends Serializable, D extends BaseDaoImpl<T, PK>> implements
        BaseEntityManager<T, PK> {

	/**
	 * 对应的Dao层对象
	 */
    protected D baseDao = null;

    protected void setBaseDao(D baseDao){
        this.baseDao = baseDao;
    }

    protected Logger logger = LoggerFactory.getLogger(BaseEntityManagerImpl.class);

    /**
     * 查找表中的所有记录， 包括禁用的 isValid = 'F' 的记录, 如果没有isValid这个字段也可以使用
     *
     * @return 表中的所有记录， 包括禁用的 isValid = 'F' 的记录, 如果没有isValid这个字段也可以使用
     */
    @Override
    @Transactional
    public List<T> listObjects() {
        return baseDao.listObjects();
    }

    /**
     * 根据对象的主键 获得数据库中对应的对象信息
     *
     * @param id PK
     * @return 数据库中对应的对象信息
     */
    @Override
    @Transactional
    public T getObjectById(PK id) {
        return baseDao.getObjectById(id);
    }

    /**
     * 保存泛型参数对象
     *
     * @param o T
     * @return Serializable
     */
    @Override
    @Transactional
    public void saveNewObject(T o) {
        baseDao.saveNewObject(o);
    }

    /**
     * 更新泛型参数对象
     *
     * @param o T
     */
    @Override
    @Transactional
    public void updateObject(T o) {
        baseDao.updateObject(o);
    }

    /**
     * 保存泛型参数对象
     *
     * @param o T
     */
    @Override
    @Transactional
    public void mergeObject(T o) {
        baseDao.mergeObject(o);
    }

    /**
     * 删除泛型参数对象
     *
     * @param o T
     */
    @Override
    @Transactional
    public void deleteObject(T o) {
        baseDao.deleteObject(o);
    }

    /**
     * 根据主键删除泛型参数对象
     *
     * @param id PK
     */
    @Override
    @Transactional
    public void deleteObjectById(PK id) {
        baseDao.deleteObjectById(id);
    }

    /**
     * @param shql      sql,hql语句
     * @param filterMap 过滤条件
     * @return listObjects
     */
    @Override
    @Transactional
    public List<T> listObjects(String shql, Map<String, Object> filterMap) {
        return baseDao.listObjects();
    }

    /**
     * @param filterMap 过滤条件
     * @return listObjects
     */
    @Override
    @Transactional
    public List<T> listObjects(Map<String, Object> filterMap) {
        return baseDao.listObjects(filterMap);
    }

    /**
     * 配合 EC Table 设计的一个查询语句
     *
     * @param shql      sql,hql语句
     * @param filterMap 过滤条件
     * @param pageDesc  分页属性
     * @return listObjects
     */
    @Override
    @Transactional
    public List<T> listObjects(String shql, Map<String, Object> filterMap, PageDesc pageDesc) {
        return baseDao.pageQuery(shql,filterMap,pageDesc);
    }

    /**
     * 配合 EC Table 设计的一个查询语句，将 filterMap 组装成对应的Hql语句 调用对应的 getObjects
     *
     * @param filterMap 过滤条件
     * @param pageDesc  分页属性
     * @return listObjects
     */
    @Override
    @Transactional
    public List<T> listObjects(Map<String, Object> filterMap, PageDesc pageDesc) {
        return baseDao.pageQuery(filterMap,pageDesc);
    }

    /**
     * 根据唯一属性值返回对象
     *
     * @param propertyName  字段名
     * @param propertyValue 值
     * @return 唯一属性值返回对象
     */
    @Override
    @Transactional
    public T getObjectByProperty(String propertyName, Object propertyValue) {
        return baseDao.getObjectByProperties(
                QueryUtils.createSqlParamsMap(propertyName, propertyValue));
    }

    /**
     * 根据多个属性返回唯一对象
     *
     * @param properties map 字段
     * @return 多个属性返回唯一对象
     */
    @Override
    @Transactional
    public T getObjectByProperties(Map<String, Object> properties) {
        return baseDao.getObjectByProperties(properties);
    }
}
