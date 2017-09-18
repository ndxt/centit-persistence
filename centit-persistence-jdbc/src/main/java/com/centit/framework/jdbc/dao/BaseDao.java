package com.centit.framework.jdbc.dao;

import com.centit.framework.core.dao.PageDesc;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
@SuppressWarnings("unused")
public interface BaseDao<T extends Serializable, PK extends Serializable>
{
    Long getSequenceNextValue(final String sequenceName);

    void deleteObject(T o);

    void deleteObjectById(PK id);

    void saveNewObject(T o);

    T getObjectById(PK id) ;

    T getObjectByProperties(Map<String, Object> properties);

    List<T> listObjectsByProperties(Map<String, Object> filterMap);

    List<T> listObjects(Map<String, Object> filterMap, PageDesc pageDesc);

    void updateObject(T o);

    void mergeObject(T o) ;

    List<T> listObjects();

    int pageCount(String sql, Map<String, Object> filterMap) ;

    int pageCount(Map<String, Object> filterMap);

    List<T> pageQuery(String sql, Map<String, Object> filterMap);

    List<T> pageQuery(Map<String, Object> filterMap);

}
