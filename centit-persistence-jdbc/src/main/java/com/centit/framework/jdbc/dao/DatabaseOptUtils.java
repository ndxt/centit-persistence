package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.database.orm.OrmDaoUtils;
import com.centit.support.database.utils.*;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

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
public abstract class DatabaseOptUtils {

    protected static Logger logger = LoggerFactory.getLogger(DatabaseOptUtils.class);

    public static Object callFunction(BaseDaoImpl<?, ?> baseDao , String procName,
                                            int sqlType, Object... paramObjs){
        try {
            return baseDao.getJdbcTemplate().execute(
                    (ConnectionCallback<Object>) conn ->
                            DatabaseAccess.callFunction(conn, procName, sqlType, paramObjs));
        } catch (DataAccessException e){
            throw new PersistenceException(PersistenceException.DATABASE_SQL_EXCEPTION, e);
        }
    }

    public final static boolean callProcedure(BaseDaoImpl<?, ?> baseDao , String procName, Object... paramObjs){
        try {
            return baseDao.getJdbcTemplate().execute(
                    (ConnectionCallback<Boolean>) conn ->
                            DatabaseAccess.callProcedure(conn, procName, paramObjs));
        } catch (DataAccessException e){
            throw new PersistenceException(PersistenceException.DATABASE_SQL_EXCEPTION, e);
        }
    }

    public final static boolean doExecuteSql(BaseDaoImpl<?, ?> baseDao , String sSql) throws DataAccessException {
        baseDao.getJdbcTemplate().execute(sSql);
        return true;
        /*try {
            return baseDao.getJdbcTemplate().execute(
                    (ConnectionCallback<Boolean>) conn ->
                            DatabaseAccess.doExecuteSql(conn,sSql));
        } catch (DataAccessException e){
            throw new PersistenceException(PersistenceException.DATABASE_SQL_EXCEPTION, e);
        }*/
    }

    /*
     * 直接运行行带参数的 SQL,update delete insert
     */
    public final static int doExecuteSql(BaseDaoImpl<?, ?> baseDao , String sSql, Object[] values) throws DataAccessException {

        return baseDao.getJdbcTemplate().update(sSql,values );
        /*try {
            return baseDao.getJdbcTemplate().execute(
                    (ConnectionCallback<Integer>) conn ->
                            DatabaseAccess.doExecuteSql(conn, sSql, values));
        } catch (DataAccessException e){
            throw new PersistenceException(PersistenceException.DATABASE_SQL_EXCEPTION, e);
        }*/
    }

    /*
     * 执行一个带命名参数的sql语句
     */
    public final static int doExecuteNamedSql(BaseDaoImpl<?, ?> baseDao , String sSql, Map<String, Object> values)
            throws DataAccessException {
        QueryAndParams qap = QueryAndParams.createFromQueryAndNamedParams(new QueryAndNamedParams(sSql, values));
        return doExecuteSql(baseDao, qap.getQuery(), qap.getParams());
    }

    /**
     * 在sql语句中找到属性对应的字段语句
     * @param querySql sql语句
     * @param fieldName 属性
     * @return 返回的对应这个属性的语句，如果找不到返回 null
     */
    public static String mapFieldToColumnPiece(String querySql, String fieldName){
        List<Pair<String,String>> fields = QueryUtils.getSqlFieldNamePieceMap(querySql);
        for(Pair<String,String> field : fields ){
            if(fieldName.equalsIgnoreCase(field.getLeft()) ||
                    fieldName.equals(DatabaseAccess.mapColumnNameToField(field.getKey())) ||
                    fieldName.equalsIgnoreCase(field.getRight())){
                return  field.getRight();
            }
        }
        return null;
    }

    /* 下面所有的查询都返回 jsonArray */

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql, String[] fieldNames , String queryCountSql,
                                            Map<String, Object> namedParams, PageDesc pageDesc /*,
                                      Map<String,KeyValuePair<String,String>> dictionaryMapInfo*/ ) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                DatabaseAccess.getScalarObjectQuery(
                                        conn, queryCountSql, namedParams)));
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, querySql,
                                namedParams, fieldNames, pageDesc.getPageNo(), pageDesc.getPageSize());
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql,  String[] fieldNames ,
                                            Map<String, Object> namedParams,  PageDesc pageDesc) {

        return listObjectsByNamedSqlAsJson(baseDao, querySql, fieldNames ,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql ), namedParams, pageDesc );
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql,  String[] fieldNames ,
                                                   Map<String, Object> namedParams) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, querySql,
                                namedParams, fieldNames);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql,  String queryCountSql,
                                            Map<String, Object> namedParams,  PageDesc pageDesc ) {
        return listObjectsByNamedSqlAsJson(baseDao, querySql, null ,  queryCountSql, namedParams,   pageDesc  );
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,  Map<String,Object> params ) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, querySql, params);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                            Map<String, Object> namedParams,  PageDesc pageDesc  ) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return DatabaseOptUtils.listObjectsByNamedSqlAsJson(baseDao, querySql, null ,
                    QueryUtils.buildGetCountSQLByReplaceFields( querySql ), namedParams,   pageDesc  );
        }else{
            return DatabaseOptUtils.listObjectsByNamedSqlAsJson(baseDao, querySql, namedParams);
        }
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames,
                                                   String queryCountSql, Object[] params,  PageDesc pageDesc ) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                DatabaseAccess.getScalarObjectQuery(
                                        conn, queryCountSql, params)));
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql ,
                                params, fieldNames, pageDesc.getPageNo(), pageDesc.getPageSize());
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql,  String[] fieldNames ,
                                                   Object[] params) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql,
                                params, fieldNames);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql,  String[] fieldNames ,
                                                   Object[] params, PageDesc pageDesc) {
        return listObjectsBySqlAsJson(baseDao, querySql, fieldNames ,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql ), params, pageDesc );
    }


    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,  String queryCountSql,
                                            Object[] params, PageDesc pageDesc ) {

        return listObjectsBySqlAsJson(baseDao,  querySql, null,  queryCountSql, params,   pageDesc );
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,  Object[] params, String[] fieldnames) {

        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql, params, fieldnames);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,  Object[] params ) {

        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql, params);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params,  PageDesc pageDesc  ) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return DatabaseOptUtils.listObjectsBySqlAsJson(baseDao, querySql,
                    QueryUtils.buildGetCountSQLByReplaceFields( querySql ), params,   pageDesc  );
        }else{
            return DatabaseOptUtils.listObjectsBySqlAsJson(baseDao, querySql, params);
        }
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql , Object[] params ) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<List<Object[]>>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsBySql(conn, querySql ,
                                params);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql, Object[] params,  PageDesc pageDesc  ) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return baseDao.getJdbcTemplate().execute(
                    (ConnectionCallback<List<Object[]>>) conn -> {
                        try {
                            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                    DatabaseAccess.getScalarObjectQuery(
                                            conn, queryCountSql, params)));
                            return DatabaseAccess.findObjectsBySql(conn, querySql ,
                                    params, pageDesc.getPageNo(), pageDesc.getPageSize());
                        } catch (SQLException | IOException e) {
                            throw new PersistenceException(e);
                        }
                    });
        }else{
            return DatabaseOptUtils.listObjectsBySql(baseDao,querySql,params);
        }
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql , Object[] params,  PageDesc pageDesc  ) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            String queryCountSql = QueryUtils.buildGetCountSQL(querySql);
            return DatabaseOptUtils.listObjectsBySql(baseDao,querySql, queryCountSql,params, pageDesc);
        }else{
            return DatabaseOptUtils.listObjectsBySql(baseDao,querySql,params);
        }
    }

    public static List<Object[]> listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql , Map<String, Object> namedParams) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<List<Object[]>>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSql(conn, querySql ,
                                namedParams);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static List<Object[]> listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql,
                                                       Map<String, Object> namedParams,  PageDesc pageDesc  ) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return baseDao.getJdbcTemplate().execute(
                    (ConnectionCallback<List<Object[]>>) conn -> {
                        try {
                            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                    DatabaseAccess.getScalarObjectQuery(
                                            conn, queryCountSql, namedParams)));
                            return DatabaseAccess.findObjectsByNamedSql(conn, querySql ,
                                    namedParams, pageDesc.getPageNo(), pageDesc.getPageSize());
                        } catch (SQLException | IOException e) {
                            throw new PersistenceException(e);
                        }
                    });
        }else{
            return DatabaseOptUtils.listObjectsByNamedSql(baseDao,querySql,namedParams);
        }
    }

    public static List<Object[]> listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql ,
                                                       Map<String, Object> namedParams,  PageDesc pageDesc  ) {
        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            String queryCountSql = QueryUtils.buildGetCountSQL(querySql);
            return DatabaseOptUtils.listObjectsByNamedSql(baseDao,querySql, queryCountSql,namedParams, pageDesc);
        }else{
            return DatabaseOptUtils.listObjectsByNamedSql(baseDao,querySql,namedParams);
        }
    }
    /**
     * 参数驱动sql查询
     * @param baseDao 任意dao对象，需要用dao中的session访问数据库
     * @param querySql 查询语句：参数驱动sql，不需要写分页查询，框架会自动转换为分页查询
     * @param fieldNames 这个是返回结果放到json中的属性名，这个不是必须的，缺省是通过sql语句中的字段名自动转换成小驼峰的属性名
     * @param queryCountSql 查询总数的参数驱动sql语句，这个也不是必须的，如果缺省，系统会自动根据查询语句来生成
     * @param namedParams 这个是前台输入的 查询参数
     * @param pageDesc 这个式前台输入的 分页信息
     * @return JSONArray
     */
    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                        String querySql, String[] fieldNames , String queryCountSql,
                                                        Map<String, Object> namedParams, PageDesc pageDesc  ) {
        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);
        Map<String, Object> paramsMap = qap.getParams();
        QueryAndNamedParams countQap = QueryUtils.translateQuery( queryCountSql, namedParams);
        paramsMap.putAll(countQap.getParams());

        return listObjectsByNamedSqlAsJson(baseDao, qap.getQuery(), fieldNames , countQap.getQuery(),
                paramsMap, pageDesc);

    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql,  String[] fieldNames ,
                                                   Map<String, Object> namedParams,  PageDesc pageDesc) {
        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);
        
        return listObjectsByNamedSqlAsJson(baseDao,  qap.getQuery(), fieldNames ,
                QueryUtils.buildGetCountSQLByReplaceFields( qap.getQuery() ), qap.getParams(),   pageDesc  );
    }

    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql,  String[] fieldNames ,
                                                   Map<String, Object> namedParams) {
        
        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);

        return listObjectsByNamedSqlAsJson( baseDao,
                qap.getQuery(),  fieldNames, qap.getParams());
    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql,  String queryCountSql,
                                                   Map<String, Object> namedParams,  PageDesc pageDesc ) {
        
        return listObjectsByParamsDriverSqlAsJson(baseDao, querySql, 
                null ,  queryCountSql, namedParams,   pageDesc  );
    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                               Map<String,Object> namedParams ) {

        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);

        return listObjectsByNamedSqlAsJson( baseDao, qap.getQuery(), qap.getParams());
    }


    /**
     * 参数驱动sql查询
     * @param baseDao 任意dao对象，需要用dao中的session访问数据库
     * @param querySql 查询语句：参数驱动sql，不需要写分页查询，框架会自动转换为分页查询
     * @param namedParams 这个是前台输入的 查询参数
     * @param pageDesc 这个式前台输入的 分页信息
     * @return JSONArray
     */
    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                            Map<String, Object> namedParams,  PageDesc pageDesc  ) {
        QueryAndNamedParams qap = QueryUtils.translateQuery( querySql, namedParams);

        return listObjectsByNamedSqlAsJson(baseDao, qap.getQuery(),
                                        qap.getParams(), pageDesc);
    }


    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Object[] params, String [] fieldName) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONObject>) conn -> {
                    try {
                        return DatabaseAccess.getObjectAsJSON(conn, querySql, params,fieldName);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Object[] params) {
        return getObjectBySqlAsJson( baseDao,  querySql, params, null);
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Map<String, Object> params, String [] fieldName) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONObject>) conn -> {
                    try {
                        return DatabaseAccess.getObjectAsJSON(conn, querySql, params, fieldName);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Map<String, Object>  params) {
        return getObjectBySqlAsJson( baseDao,  querySql, params, null);
    }

    public  <T> T getObjectCascadeById(BaseDaoImpl<?, ?> baseDao, Object id, final Class<T> type) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectCascadeById(conn, id, type));
    }


    public  <T> T getObjectCascadeShallowById(BaseDaoImpl<?, ?> baseDao, Object id, final Class<T> type) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectCascadeShallowById(conn, id, type));
    }

    public <T> T fetchObjectReference(BaseDaoImpl<?, ?> baseDao, T o, String columnName) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectReference(conn, o, columnName));
    }

    public <T> T fetchObjectReferences(BaseDaoImpl<?, ?> baseDao, T o) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectReferences(conn, o));
    }

    public <T> Integer saveObjectReference(BaseDaoImpl<?, ?> baseDao, T o, String columnName) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveObjectReference(conn, o, columnName));
    }

    public <T> Integer saveObjectReferences(BaseDaoImpl<?, ?> baseDao, T o) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveObjectReferences(conn, o));
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONObject>) conn -> {
                    try {
                        return DatabaseAccess.getObjectAsJSON(conn, querySql);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql,
                                                    Map<String,Object> values){
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sSql,values);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }
    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao,
                                                    String sSql, Object[] values) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sSql,values);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql)
            throws SQLException, IOException {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sSql);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql,Object value)
            throws SQLException, IOException {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Object>) conn -> {
                    try {
                        return DatabaseAccess.getScalarObjectQuery(conn, sSql, value);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static Long getSequenceNextValue(BaseDaoImpl<?, ?> baseDao, String sequenceName){
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Long>) conn ->
                        OrmDaoUtils.getSequenceNextValue(conn, sequenceName));
    }


    /**
     * 保存任意对象
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 保存任意对象数量
     */
    public static int batchSaveNewObjects(BaseDaoImpl<?, ?> baseDao,
                                             Collection<? extends Object> objects) {

        return baseDao.getJdbcTemplate().execute(
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
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 更新对象数量
     */
    public static int batchUpdateObjects(BaseDaoImpl<?, ?> baseDao,
                                                Collection<? extends Object> objects) {

        return baseDao.getJdbcTemplate().execute(
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
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return merge对象数量
     */
    public static int batchMergeObjects(BaseDaoImpl<?, ?> baseDao,
                                               Collection<? extends Object> objects) {

        return baseDao.getJdbcTemplate().execute(
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
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 批量删除对象数量
     */
    public static int batchDeleteObjects(BaseDaoImpl<?, ?> baseDao,
                                              Collection<? extends Object> objects) {

        return baseDao.getJdbcTemplate().execute(
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
     * @param baseDao BaseDaoImpl
     * @param fields 需要修改的属性，对应的值从 object 对象中找
     * @param object   对应 fields 中的属性必须有值，如果没有值 将被设置为null
     * @param properties 对应的过滤条件， 属性名 和 属性值 ，必须是 等于匹配
     * @param <T> 泛型对象类型 这个对象必须要有jpi注解
     * @return Integer 修改的数据库行数
     */
    public <T> Integer batchUpdateObject(BaseDaoImpl<?, ?> baseDao, final Collection<String> fields,
                                         final T object, final Map<String, Object> properties) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.batchUpdateObject(conn, fields, object, properties));
    }
    /**
     * 批量修改对象
     * @param baseDao BaseDaoImpl
     * @param fields 需要修改的属性，对应的值从 object 对象中找
     * @param object   对应 fields 中的属性必须有值，如果没有值 将被设置为null
     * @param properties 对应的过滤条件， 属性名 和 属性值 ，必须是 等于匹配
     * @param <T> 泛型对象类型 这个对象必须要有jpi注解
     * @return Integer 修改的数据库行数
     **/
    public <T> Integer batchUpdateObject(BaseDaoImpl<?, ?> baseDao, String[] fields,
                                          T object, Map<String, Object> properties) {
        return batchUpdateObject(baseDao, CollectionsOpt.arrayToList(fields), object, properties);
    }

    /**
     * 批量修改 对象
     * @param baseDao BaseDaoImpl
     * @param type 对象类型
     * @param propertiesValue 值对
     * @param propertiesFilter 过滤条件对
     * @return 更改的条数
     */
    public static Integer batchUpdateObject(
            BaseDaoImpl<?, ?> baseDao, Class<?> type,
            Map<String, Object> propertiesValue,
            Map<String, Object> propertiesFilter) {
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.batchUpdateObject(conn, type, propertiesValue, propertiesFilter));
    }

}
