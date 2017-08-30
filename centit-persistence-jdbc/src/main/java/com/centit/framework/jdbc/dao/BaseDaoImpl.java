package com.centit.framework.jdbc.dao;

import com.centit.framework.core.dao.PageDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public abstract class BaseDaoImpl<T extends Serializable, PK extends Serializable> 
{

    
    protected static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);

    public abstract void deleteObject(T o);

    public abstract void deleteObjectById(PK id);

    public abstract void saveNewObject(T o);

    public abstract T getObjectById(PK id) ;

    public abstract T getObjectByProperties(Map<String, Object> properties);

    public abstract void updateObject(T o);

    public abstract void mergeObject(T o) ;

    public abstract List<T> listObjects();

    public abstract int pageCount(String sql, Map<String, Object> filterMap) ;

    public abstract int pageCount(Map<String, Object> filterMap) ;

    public abstract List<T> pageQuery(String sql, Map<String, Object> filterMap);

    public abstract List<T> pageQuery(Map<String, Object> filterMap) ;

    public abstract List<T> listObjects(Map<String, Object> filterMap, PageDesc pageDesc);


}
