package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmDaoUtils;
import com.centit.support.database.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 意图将BaseDao中公共的部分独立出来，减少类的函数数量，
 * 因为每一个继承BaseDaoImpl的类都有这些函数，而这些行数基本上都是一样的
 */
@SuppressWarnings("unused")
public abstract class JdbcTemplateUtils {

    protected static Logger logger = LoggerFactory.getLogger(JdbcTemplateUtils.class);

    /**
     * @param jdbcTemplate  数据库链接
     * @param procName procName
     * @param sqlType
     *            返回值类型
     * @param paramObjs paramObjs
     * @return  调用数据库函数
     * */
    public static Object callFunction(JdbcTemplate jdbcTemplate, String procName,
                                      int sqlType, Object... paramObjs){
        try {
            return jdbcTemplate.execute(
                    (ConnectionCallback<Object>) conn ->
                            DatabaseAccess.callFunction(conn, procName, sqlType, paramObjs));
        } catch (DataAccessException e){
            throw new ObjectException(ObjectException.DATABASE_SQL_EXCEPTION, e);
        }
    }

    public final static boolean callProcedure(JdbcTemplate jdbcTemplate, String procName, Object... paramObjs){
        try {
            return jdbcTemplate.execute(
                    (ConnectionCallback<Boolean>) conn ->
                            DatabaseAccess.callProcedure(conn, procName, paramObjs));
        } catch (DataAccessException e){
            throw new ObjectException(ObjectException.DATABASE_SQL_EXCEPTION, e);
        }
    }

    public final static boolean doExecuteSql(JdbcTemplate jdbcTemplate, String sSql) throws DataAccessException {
        jdbcTemplate.execute(sSql);
        return true;
        /*try {
            return jdbcTemplate.execute(
                    (ConnectionCallback<Boolean>) conn ->
                            DatabaseAccess.doExecuteSql(conn,sSql));
        } catch (DataAccessException e){
            throw new ObjectException(ObjectException.DATABASE_SQL_EXCEPTION, e);
        }*/
    }

    /*
     * 直接运行行带参数的 SQL,update delete insert
     */
    public final static int doExecuteSql(JdbcTemplate jdbcTemplate, String sSql, Object[] values) throws DataAccessException {

        return jdbcTemplate.update(sSql,values);
        /*try {
            return jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            DatabaseAccess.doExecuteSql(conn, sSql, values));
        } catch (DataAccessException e){
            throw new ObjectException(ObjectException.DATABASE_SQL_EXCEPTION, e);
        }*/
    }

    /*
     * 执行一个带命名参数的sql语句
     */
    public final static int doExecuteNamedSql(JdbcTemplate jdbcTemplate, String sSql, Map<String, Object> values)
            throws DataAccessException {
        QueryAndParams qap = QueryAndParams.createFromQueryAndNamedParams(new QueryAndNamedParams(sSql, values));
        return doExecuteSql(jdbcTemplate, qap.getQuery(), qap.getParams());
    }

    /* 下面所有的查询都返回 jsonArray */

    public static JSONArray listObjectsByNamedSqlAsJson(JdbcTemplate jdbcTemplate,
                                            String querySql, String[] fieldNames, String queryCountSql,
                                            Map<String, Object> namedParams, PageDesc pageDesc /*,
                                      Map<String,LeftRightPair<String,String>> dictionaryMapInfo*/) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                DatabaseAccess.getScalarObjectQuery(
                                        conn, queryCountSql, namedParams)));
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, querySql,
                                namedParams, fieldNames, pageDesc.getPageNo(), pageDesc.getPageSize());
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONArray listObjectsByNamedSqlAsJson(JdbcTemplate jdbcTemplate,
                                            String querySql, String[] fieldNames,
                                            Map<String, Object> namedParams, PageDesc pageDesc) {

        return listObjectsByNamedSqlAsJson(jdbcTemplate, querySql, fieldNames,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql), namedParams, pageDesc);
    }

    public static JSONArray listObjectsByNamedSqlAsJson(JdbcTemplate jdbcTemplate,
                                                   String querySql, String[] fieldNames,
                                                   Map<String, Object> namedParams) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, querySql,
                                namedParams, fieldNames);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONArray listObjectsByNamedSqlAsJson(JdbcTemplate jdbcTemplate,
                                            String querySql, String queryCountSql,
                                            Map<String, Object> namedParams, PageDesc pageDesc) {
        return listObjectsByNamedSqlAsJson(jdbcTemplate, querySql, null, queryCountSql, namedParams, pageDesc);
    }

    public static JSONArray listObjectsByNamedSqlAsJson(JdbcTemplate jdbcTemplate, String querySql, Map<String,Object> params) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, querySql, params);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONArray listObjectsByNamedSqlAsJson(JdbcTemplate jdbcTemplate, String querySql,
                                            Map<String, Object> namedParams, PageDesc pageDesc) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return JdbcTemplateUtils.listObjectsByNamedSqlAsJson(jdbcTemplate, querySql, null,
                    QueryUtils.buildGetCountSQLByReplaceFields(querySql), namedParams, pageDesc);
        }else{
            JSONArray ja = JdbcTemplateUtils.listObjectsByNamedSqlAsJson(jdbcTemplate, querySql, namedParams);
            if(ja != null && pageDesc != null){
                pageDesc.noPaging(ja.size());
            }
            return ja;
        }
    }

    public static JSONArray listObjectsBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql, String[] fieldNames,
                                                   String queryCountSql, Object[] params, PageDesc pageDesc) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                DatabaseAccess.getScalarObjectQuery(
                                        conn, queryCountSql, params)));
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql,
                                params, fieldNames, pageDesc.getPageNo(), pageDesc.getPageSize());
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(JdbcTemplate jdbcTemplate,
                                                   String querySql, String[] fieldNames,
                                                   Object[] params) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql,
                                params, fieldNames);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(JdbcTemplate jdbcTemplate,
                                                   String querySql, String[] fieldNames,
                                                   Object[] params, PageDesc pageDesc) {
        return listObjectsBySqlAsJson(jdbcTemplate, querySql, fieldNames,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql), params, pageDesc);
    }


    public static JSONArray listObjectsBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql, String queryCountSql,
                                            Object[] params, PageDesc pageDesc) {

        return listObjectsBySqlAsJson(jdbcTemplate, querySql, null, queryCountSql, params, pageDesc);
    }

    public static JSONArray listObjectsBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql, Object[] params, String[] fieldnames) {

        return jdbcTemplate.execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql, params, fieldnames);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql, Object[] params) {

        return jdbcTemplate.execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql, params);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql, Object[] params, PageDesc pageDesc) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return JdbcTemplateUtils.listObjectsBySqlAsJson(jdbcTemplate, querySql,
                    QueryUtils.buildGetCountSQLByReplaceFields( querySql), params, pageDesc);
        }else{
            JSONArray ja = JdbcTemplateUtils.listObjectsBySqlAsJson(jdbcTemplate, querySql, params);
            if(ja != null && pageDesc != null){
                pageDesc.noPaging(ja.size());
            }
            return ja;
        }
    }

    public static List<Object[]> listObjectsBySql(JdbcTemplate jdbcTemplate, String querySql, Object[] params) {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<Object[]>>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsBySql(conn, querySql,
                                params);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static List<Object[]> listObjectsBySql(JdbcTemplate jdbcTemplate, String querySql, String queryCountSql, Object[] params, PageDesc pageDesc) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return jdbcTemplate.execute(
                    (ConnectionCallback<List<Object[]>>) conn -> {
                        try {
                            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                    DatabaseAccess.getScalarObjectQuery(
                                            conn, queryCountSql, params)));
                            return DatabaseAccess.findObjectsBySql(conn, querySql,
                                    params, pageDesc.getPageNo(), pageDesc.getPageSize());
                        } catch (SQLException | IOException e) {
                            throw new ObjectException(e);
                        }
                    });
        }else{
            List<Object[]> ja = JdbcTemplateUtils.listObjectsBySql(jdbcTemplate,querySql,params);
            if(ja != null && pageDesc != null){
                pageDesc.noPaging(ja.size());
            }
            return ja;
        }
    }

    public static List<Object[]> listObjectsBySql(JdbcTemplate jdbcTemplate, String querySql, Object[] params, PageDesc pageDesc) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            String queryCountSql = QueryUtils.buildGetCountSQL(querySql);
            return JdbcTemplateUtils.listObjectsBySql(jdbcTemplate,querySql, queryCountSql,params, pageDesc);
        }else{
            List<Object[]> ja = JdbcTemplateUtils.listObjectsBySql(jdbcTemplate,querySql,params);
            if(ja != null && pageDesc != null){
                pageDesc.noPaging(ja.size());
            }
            return ja;
        }
    }

    public static List<Object[]> listObjectsByNamedSql(JdbcTemplate jdbcTemplate, String querySql, Map<String, Object> namedParams) {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<Object[]>>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSql(conn, querySql,
                                namedParams);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static List<Object[]> listObjectsByNamedSql(JdbcTemplate jdbcTemplate, String querySql, String queryCountSql,
                                                       Map<String, Object> namedParams, PageDesc pageDesc) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return jdbcTemplate.execute(
                    (ConnectionCallback<List<Object[]>>) conn -> {
                        try {
                            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                    DatabaseAccess.getScalarObjectQuery(
                                            conn, queryCountSql, namedParams)));
                            return DatabaseAccess.findObjectsByNamedSql(conn, querySql,
                                    namedParams, pageDesc.getPageNo(), pageDesc.getPageSize());
                        } catch (SQLException | IOException e) {
                            throw new ObjectException(e);
                        }
                    });
        }else{
            List<Object[]> ja = JdbcTemplateUtils.listObjectsByNamedSql(jdbcTemplate,querySql,namedParams);
            if(ja != null && pageDesc != null){
                pageDesc.noPaging(ja.size());
            }
            return ja;
        }
    }

    public static List<Object[]> listObjectsByNamedSql(JdbcTemplate jdbcTemplate, String querySql,
                                                       Map<String, Object> namedParams, PageDesc pageDesc) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            String queryCountSql = QueryUtils.buildGetCountSQL(querySql);
            return JdbcTemplateUtils.listObjectsByNamedSql(jdbcTemplate,querySql, queryCountSql,namedParams, pageDesc);
        }else{
            List<Object[]> ja = JdbcTemplateUtils.listObjectsByNamedSql(jdbcTemplate,querySql,namedParams);
            if(ja != null && pageDesc != null){
                pageDesc.noPaging(ja.size());
            }
            return ja;
        }
    }
    /**
     * 参数驱动sql查询
     * @param jdbcTemplate 任意dao对象，需要用dao中的session访问数据库
     * @param querySql 查询语句：参数驱动sql，不需要写分页查询，框架会自动转换为分页查询
     * @param fieldNames 这个是返回结果放到json中的属性名，这个不是必须的，缺省是通过sql语句中的字段名自动转换成小驼峰的属性名
     * @param queryCountSql 查询总数的参数驱动sql语句，这个也不是必须的，如果缺省，系统会自动根据查询语句来生成
     * @param namedParams 这个是前台输入的 查询参数
     * @param pageDesc 这个式前台输入的 分页信息
     * @return JSONArray
     */
    public static JSONArray listObjectsByParamsDriverSqlAsJson(JdbcTemplate jdbcTemplate,
                                                        String querySql, String[] fieldNames, String queryCountSql,
                                                        Map<String, Object> namedParams, PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);
        Map<String, Object> paramsMap = qap.getParams();
        QueryAndNamedParams countQap = QueryUtils.translateQuery( queryCountSql, namedParams);
        paramsMap.putAll(countQap.getParams());

        return listObjectsByNamedSqlAsJson(jdbcTemplate, qap.getQuery(), fieldNames, countQap.getQuery(),
                paramsMap, pageDesc);

    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(JdbcTemplate jdbcTemplate,
                                                   String querySql, String[] fieldNames,
                                                   Map<String, Object> namedParams, PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);

        return listObjectsByNamedSqlAsJson(jdbcTemplate, qap.getQuery(), fieldNames,
                QueryUtils.buildGetCountSQLByReplaceFields( qap.getQuery()), qap.getParams(), pageDesc);
    }

    public static JSONArray listObjectsByParamsDriverSqlAsJson(JdbcTemplate jdbcTemplate,
                                                   String querySql, String[] fieldNames,
                                                   Map<String, Object> namedParams) {

        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);

        return listObjectsByNamedSqlAsJson( jdbcTemplate,
                qap.getQuery(), fieldNames, qap.getParams());
    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(JdbcTemplate jdbcTemplate,
                                                   String querySql, String queryCountSql,
                                                   Map<String, Object> namedParams, PageDesc pageDesc) {

        return listObjectsByParamsDriverSqlAsJson(jdbcTemplate, querySql,
                null, queryCountSql, namedParams, pageDesc);
    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(JdbcTemplate jdbcTemplate, String querySql,
                                                               Map<String,Object> namedParams) {

        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);

        return listObjectsByNamedSqlAsJson( jdbcTemplate, qap.getQuery(), qap.getParams());
    }


    /**
     * 参数驱动sql查询
     * @param jdbcTemplate 任意dao对象，需要用dao中的session访问数据库
     * @param querySql 查询语句：参数驱动sql，不需要写分页查询，框架会自动转换为分页查询
     * @param namedParams 这个是前台输入的 查询参数
     * @param pageDesc 这个式前台输入的 分页信息
     * @return JSONArray
     */
    public static JSONArray listObjectsByParamsDriverSqlAsJson(JdbcTemplate jdbcTemplate, String querySql,
                                            Map<String, Object> namedParams, PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);

        return listObjectsByNamedSqlAsJson(jdbcTemplate, qap.getQuery(),
                                        qap.getParams(), pageDesc);
    }


    public static JSONObject getObjectBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql,
                                                  Object[] params, String [] fieldName) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONObject>) conn -> {
                    try {
                        return DatabaseAccess.getObjectAsJSON(conn, querySql, params,fieldName);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONObject getObjectBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql,
                                                  Object[] params) {
        return getObjectBySqlAsJson( jdbcTemplate, querySql, params, null);
    }

    public static JSONObject getObjectBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql,
                                                  Map<String, Object> params, String [] fieldName) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONObject>) conn -> {
                    try {
                        return DatabaseAccess.getObjectAsJSON(conn, querySql, params, fieldName);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static JSONObject getObjectBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql,
                                                  Map<String, Object>  params) {
        return getObjectBySqlAsJson( jdbcTemplate, querySql, params, null);
    }

    public static JSONObject getObjectBySqlAsJson(JdbcTemplate jdbcTemplate, String querySql) {
        return jdbcTemplate.execute(
                (ConnectionCallback<JSONObject>) conn -> {
                    try {
                        return DatabaseAccess.getObjectAsJSON(conn, querySql);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }


    public static Object getScalarObjectQuery(JdbcTemplate jdbcTemplate, String sNamedSql,
                                                    Map<String,Object> values){
        return jdbcTemplate.execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sNamedSql, values);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }
    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(JdbcTemplate jdbcTemplate,
                                                    String sSql, Object[] values) {
        return jdbcTemplate.execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sSql,values);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(JdbcTemplate jdbcTemplate, String sSql)
            throws SQLException, IOException {
        return jdbcTemplate.execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sSql);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(JdbcTemplate jdbcTemplate, String sSql,Object value)
            throws SQLException, IOException {
        return jdbcTemplate.execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sSql, value);
                    } catch (SQLException | IOException e) {
                        throw new ObjectException(e);
                    }
                });
    }

    public static Long getSequenceNextValue(JdbcTemplate jdbcTemplate, String sequenceName){
        return jdbcTemplate.execute(
                (ConnectionCallback<Long>) conn ->
                        OrmDaoUtils.getSequenceNextValue(conn, sequenceName));
    }


    /**
     * 保存任意对象
     * @param jdbcTemplate BaseDaoImpl
     * @param objects Collection objects
     * @return 保存任意对象数量
     */
    public static int batchSaveNewObjects(JdbcTemplate jdbcTemplate,
                                             Collection<?> objects) {

        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn -> {
                    int successSaved=0;
                    for(Object o : objects) {
                        successSaved += OrmDaoUtils.saveNewObject(conn, o);
                    }
                    return successSaved;
                });
    }

    /**
     * 更新任意对象
     * @param jdbcTemplate BaseDaoImpl
     * @param objects Collection objects
     * @return 更新对象数量
     */
    public static int batchUpdateObjects(JdbcTemplate jdbcTemplate,
                                                Collection<?> objects) {

        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn -> {
                    int successUpdated=0;
                    for(Object o : objects) {
                        successUpdated += OrmDaoUtils.updateObject(conn, o);
                    }
                    return successUpdated;
                });
    }


    /**
     * 保存或者更新任意对象 ，每次都先判断是否存在
     * @param jdbcTemplate BaseDaoImpl
     * @param objects Collection objects
     * @return merge对象数量
     */
    public static int batchMergeObjects(JdbcTemplate jdbcTemplate,
                                               Collection<?> objects) {

        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn -> {
                    int successMerged=0;
                    for(Object o : objects) {
                        successMerged += OrmDaoUtils.mergeObject(conn, o);
                    }
                    return successMerged;
                });
    }

    /**
     * 批量删除对象
     * @param jdbcTemplate BaseDaoImpl
     * @param objects Collection objects
     * @return 批量删除对象数量
     */
    public static int batchDeleteObjects(JdbcTemplate jdbcTemplate,
                                              Collection<?> objects) {

        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn -> {
                    int successDeleted=0;
                    for(Object o : objects) {
                        successDeleted += OrmDaoUtils.deleteObject(conn, o);
                    }
                    return successDeleted;
                });
    }

    /**
     * 批量修改对象
     * @param jdbcTemplate BaseDaoImpl
     * @param fields 需要修改的属性，对应的值从 object 对象中找
     * @param object   对应 fields 中的属性必须有值，如果没有值 将被设置为null
     * @param properties 对应的过滤条件， 属性名 和 属性值 ，必须是 等于匹配
     * @param <T> 泛型对象类型 这个对象必须要有jpi注解
     * @return Integer 修改的数据库行数
     */
    public static <T> Integer batchUpdateObject(JdbcTemplate jdbcTemplate, final Collection<String> fields,
                                         final T object, final Map<String, Object> properties) {
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.batchUpdateObject(conn, fields, object, properties));
    }
    /**
     * 批量修改对象
     * @param jdbcTemplate BaseDaoImpl
     * @param fields 需要修改的属性，对应的值从 object 对象中找
     * @param object   对应 fields 中的属性必须有值，如果没有值 将被设置为null
     * @param properties 对应的过滤条件， 属性名 和 属性值 ，必须是 等于匹配
     * @param <T> 泛型对象类型 这个对象必须要有jpi注解
     * @return Integer 修改的数据库行数
     **/
    public static <T> Integer batchUpdateObject(JdbcTemplate jdbcTemplate, String[] fields,
                                          T object, Map<String, Object> properties) {
        return batchUpdateObject(jdbcTemplate, CollectionsOpt.arrayToList(fields), object, properties);
    }

    /**
     * 批量修改 对象
     * @param jdbcTemplate BaseDaoImpl
     * @param type 对象类型
     * @param propertiesValue 值对
     * @param propertiesFilter 过滤条件对
     * @return 更改的条数
     */
    public static Integer batchUpdateObject(
            JdbcTemplate jdbcTemplate, Class<?> type,
            Map<String, Object> propertiesValue,
            Map<String, Object> propertiesFilter) {
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.batchUpdateObject(conn, type, propertiesValue, propertiesFilter));
    }

    public static <T> Integer replaceObjectsAsTabulation(
        JdbcTemplate jdbcTemplate, List<T> oldDbObject,
        List<T> newObjects){
        return jdbcTemplate.execute(
            (ConnectionCallback<Integer>) conn ->
                OrmDaoUtils.replaceObjectsAsTabulation(conn, oldDbObject, newObjects));
    }

    public static Integer replaceObjectsAsTabulation(
        JdbcTemplate jdbcTemplate, Class<?> type,
        List<Map<String, Object>> oldDbObject,
        List<Map<String, Object>> newObjects){
        return jdbcTemplate.execute(
            (ConnectionCallback<Integer>) conn -> {
                JsonObjectDao dao = GeneralJsonObjectDao.createJsonObjectDao(
                    conn, JpaMetadata.fetchTableMapInfo(type));
                return dao.replaceObjectsAsTabulation(oldDbObject, newObjects);
            });
    }

    public static DBType doGetDBType(JdbcTemplate jdbcTemplate){
        return jdbcTemplate.execute(
            (ConnectionCallback<DBType>) conn -> DBType.mapDBType(conn));
    }
}
