package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.core.dao.PageDesc;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.database.orm.OrmDaoUtils;
import com.centit.support.database.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/**
 * 意图将BaseDao中公共的部分独立出来，减少类的函数数量，
 * 因为每一个继承BaseDaoImpl的类都有这些函数，而这些行数基本上都是一样的
 */
@SuppressWarnings("unused")
public abstract class DatabaseOptUtils {

    protected static Logger logger = LoggerFactory.getLogger(DatabaseOptUtils.class);

    public final static Object callFunction(BaseDaoImpl<?, ?> baseDao , String procName,
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

    public final static boolean doExecuteSql(BaseDaoImpl<?, ?> baseDao , String sSql) throws SQLException {
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
    public final static int doExecuteSql(BaseDaoImpl<?, ?> baseDao , String sSql, Object[] values) throws SQLException {

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
            throws SQLException {
        QueryAndParams qap = QueryAndParams.createFromQueryAndNamedParams(new QueryAndNamedParams(sSql, values));
        return doExecuteSql(baseDao, qap.getSql(), qap.getParams());
    }


    /* 下面所有的查询都返回 jsonArray */

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql, String[] fieldNames , String queryCountSql,
                                            Map<String, Object> filterMap, PageDesc pageDesc /*,
                                      Map<String,KeyValuePair<String,String>> dictionaryMapInfo*/ ) {

        QueryAndNamedParams queryQap = QueryUtils.translateQuery(querySql, filterMap);
        QueryAndNamedParams queryCountQap = QueryUtils.translateQuery(queryCountSql, filterMap);
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                DatabaseAccess.getScalarObjectQuery(
                                        conn, queryCountQap.getSql(), queryCountQap.getParams())));
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, queryQap.getSql(),
                                queryQap.getParams(), fieldNames, pageDesc.getPageNo(), pageDesc.getPageSize());
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql,  String[] fieldNames ,
                                            Map<String, Object> filterMap,  PageDesc pageDesc) {

        return listObjectsBySqlAsJson(baseDao, querySql, fieldNames ,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql ), filterMap,   pageDesc  );
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql,  String[] fieldNames ,
                                                   Map<String, Object> filterMap) {

        QueryAndNamedParams queryQap = QueryUtils.translateQuery(querySql, filterMap);
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, queryQap.getSql(),
                                queryQap.getParams(), fieldNames);
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }


    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql,  String queryCountSql,
                                            Map<String, Object> filterMap,  PageDesc pageDesc ) {
        return listObjectsBySqlAsJson(baseDao, querySql, null ,  queryCountSql, filterMap,   pageDesc  );
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,String querySql,
                                            Map<String, Object> filterMap,  PageDesc pageDesc ) {

        return listObjectsBySqlAsJson(baseDao, querySql, null ,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql ), filterMap,   pageDesc  );
    }



    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,  String queryCountSql,
                                            Object[] params,  PageDesc pageDesc ) {

        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                                DatabaseAccess.getScalarObjectQuery(
                                        conn, queryCountSql, params)));
                        return DatabaseAccess.findObjectsAsJSON(conn, querySql ,
                                params, null, pageDesc.getPageNo(), pageDesc.getPageSize());
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,String querySql,
                                            Object[] params,  PageDesc pageDesc ) {
        return listObjectsBySqlAsJson(baseDao, querySql, QueryUtils.buildGetCountSQLBySubSelect(querySql ),
                params , pageDesc);

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

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,  Map<String,Object> params ) {
        QueryAndNamedParams queryQap = QueryUtils.translateQuery(querySql, params);
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<JSONArray>) conn -> {
                    try {
                        return DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, queryQap.getSql(), queryQap.getParams());
                    } catch (SQLException | IOException e) {
                        throw new PersistenceException(e);
                    }
                });
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

    public final static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql,
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
    public final static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao,
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
    public final static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql)
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
    public final static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql,Object value)
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

    public final static Long getSequenceNextValue(BaseDaoImpl<?, ?> baseDao, String sequenceName){
        return baseDao.getJdbcTemplate().execute(
                (ConnectionCallback<Long>) conn ->
                        OrmDaoUtils.getSequenceNextValue(conn, sequenceName));
    }


    /**
     * 保存任意对象，hibernate 托管的对象
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 保存任意对象，hibernate 托管的对象
     */
    public final static int batchSaveNewObjects(BaseDaoImpl<?, ?> baseDao,
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
     * 保存任意对象，hibernate 托管的对象
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 保存任意对象，hibernate 托管的对象
     */
    public final static int batchUpdateObjects(BaseDaoImpl<?, ?> baseDao,
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
     * 保存任意对象，hibernate 托管的对象
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 保存任意对象，hibernate 托管的对象
     */
    public final static int batchMergeObjects(BaseDaoImpl<?, ?> baseDao,
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
}
