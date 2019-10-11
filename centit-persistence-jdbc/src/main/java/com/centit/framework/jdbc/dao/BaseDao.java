package com.centit.framework.jdbc.dao;

import com.centit.support.database.utils.PageDesc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
@SuppressWarnings("unused")
public interface BaseDao<T extends Serializable, PK extends Serializable>{

    void deleteObject(T o);

    void deleteObjectById(PK id);

    void saveNewObject(T o);

    T getObjectById(PK id) ;

    T getObjectByProperties(Map<String, Object> properties);

    List<T> listObjectsByProperties(Map<String, Object> filterMap);

    List<T> listObjects(Map<String, Object> filterMap, PageDesc pageDesc);

    int updateObject(T o);

    int mergeObject(T o) ;

    List<T> listObjects();

}
