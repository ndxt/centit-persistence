package com.centit.framework.jdbc.service;

import com.centit.framework.core.dao.PageDesc;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.support.algorithm.ReflectionOpt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Propagation;
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
     * 日志系统是否开启debug模式
     */
    protected boolean logDebug = logger.isDebugEnabled();

    @Transactional(propagation=Propagation.REQUIRED)
    public void deleteObject(T o) {
        baseDao.deleteObject(o);
    }

    @Transactional(propagation=Propagation.REQUIRED)
    public void deleteObjectById(PK id) {
        baseDao.deleteObjectById(id);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void saveObject(T o) {
        baseDao.saveNewObject(o);
    }

    /**
     * 保存泛型参数对象
     *
     * @param o T
     * @return  泛型参数对象
     */
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void saveNewObject(T o){
        /*return*/ baseDao.saveNewObject(o);
    }
    
    /**
     * 更新泛型参数对象
     *
     * @param o T
     */
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void updateObject(T o){
        baseDao.updateObject(o);
    }
    
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void mergeObject(T o) {
        baseDao.mergeObject(o);

    }
    
    

    
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public T getObjectById(PK id) {
        return baseDao.getObjectById(id);
    }


    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public T getObjectByProperties(Map<String, Object> properties) {
        return baseDao.getObjectByProperties(properties);
    }

    public void copyObjectNotNullProperty(T des, T sou) {
        ReflectionOpt.invokeBinaryOpt(des, "copyNotNullProperty", sou);
    }

    public void clearObjectProperties(T des) {
        ReflectionOpt.invokeNoParamFunc(des, "clearProperties");

    }

    public void copyObject(T des, T sou) {
        ReflectionOpt.invokeBinaryOpt(des, "copy", sou);
    }
    
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public List<T> listObjects() {
        return baseDao.listObjects();
    }


    /**
     * @param shql shql
     * @param filterMap 过滤条件
     * @return  listObjects
     */
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public abstract List<T> listObjects(String shql, Map<String, Object> filterMap) ;

    /**
     * @param filterMap 过滤条件
     * @return listObjects
     */
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public abstract List<T> listObjects(Map<String, Object> filterMap);

    
    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public List<T> listObjects(Map<String, Object> filterMap, PageDesc pageDesc) {
        return baseDao.listObjects(filterMap, pageDesc);
    }



}
