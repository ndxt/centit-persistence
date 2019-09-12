package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.database.utils.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

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
    /**
     * @param baseDao  数据库链接
     * @param procName procName
     * @param sqlType
     *            返回值类型
     * @param paramObjs paramObjs
     * @return  调用数据库函数
     * */
    public static Object callFunction(BaseDaoImpl<?, ?> baseDao, String procName,
                                            int sqlType, Object... paramObjs){
        return JdbcTemplateUtils.callFunction(baseDao.getJdbcTemplate(), procName,
                                        sqlType, paramObjs);
    }

    public final static boolean callProcedure(BaseDaoImpl<?, ?> baseDao, String procName, Object... paramObjs){
        return JdbcTemplateUtils.callProcedure(baseDao.getJdbcTemplate(), procName, paramObjs);
    }

    public final static boolean doExecuteSql(BaseDaoImpl<?, ?> baseDao, String sSql) throws DataAccessException {
        return JdbcTemplateUtils.doExecuteSql(baseDao.getJdbcTemplate(), sSql);
    }

    /*
     * 直接运行行带参数的 SQL,update delete insert
     */
    public final static int doExecuteSql(BaseDaoImpl<?, ?> baseDao, String sSql, Object[] values) throws DataAccessException {
        return JdbcTemplateUtils.doExecuteSql(baseDao.getJdbcTemplate(), sSql, values);
    }

    /*
     * 执行一个带命名参数的sql语句
     */
    public final static int doExecuteNamedSql(BaseDaoImpl<?, ?> baseDao, String sSql, Map<String, Object> values)
            throws DataAccessException {
        return JdbcTemplateUtils.doExecuteNamedSql(baseDao.getJdbcTemplate(), sSql, values);
    }

    /* 下面所有的查询都返回 jsonArray */
    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql, String[] fieldNames, String queryCountSql,
                                            Map<String, Object> namedParams, PageDesc pageDesc /*,
                                      Map<String,LeftRightPair<String,String>> dictionaryMapInfo*/) {
        return JdbcTemplateUtils.listObjectsByNamedSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, fieldNames, queryCountSql, namedParams, pageDesc);
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql, String[] fieldNames,
                                            Map<String, Object> namedParams, PageDesc pageDesc) {

        return listObjectsByNamedSqlAsJson(baseDao, querySql, fieldNames,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql), namedParams, pageDesc);
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, String[] fieldNames,
                                                   Map<String, Object> namedParams) {
        return JdbcTemplateUtils.listObjectsByNamedSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, fieldNames, namedParams);
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                            String querySql, String queryCountSql,
                                            Map<String, Object> namedParams, PageDesc pageDesc) {
        return listObjectsByNamedSqlAsJson(baseDao, querySql, null,
                queryCountSql, namedParams, pageDesc);
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                        Map<String,Object> params) {
        return JdbcTemplateUtils.listObjectsByNamedSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, params);
    }

    public static JSONArray listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                            Map<String, Object> namedParams, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsByNamedSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, namedParams, pageDesc);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames,
                                                   String queryCountSql, Object[] params, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsBySqlAsJson(baseDao.getJdbcTemplate(),
                querySql, fieldNames, queryCountSql, params, pageDesc);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, String[] fieldNames,
                                                   Object[] params) {
        return JdbcTemplateUtils.listObjectsBySqlAsJson(baseDao.getJdbcTemplate(),
                querySql, fieldNames, params);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, String[] fieldNames,
                                                   Object[] params, PageDesc pageDesc) {
        return listObjectsBySqlAsJson(baseDao, querySql, fieldNames,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql), params, pageDesc);
    }


    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql,
                                            Object[] params, PageDesc pageDesc) {
        return listObjectsBySqlAsJson(baseDao, querySql, null, queryCountSql, params, pageDesc);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, Object[] params, String[] fieldnames) {

        return JdbcTemplateUtils.listObjectsBySqlAsJson(baseDao.getJdbcTemplate(),
                querySql, params, fieldnames);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params) {
        return JdbcTemplateUtils.listObjectsBySqlAsJson(baseDao.getJdbcTemplate(),
                querySql, params);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, Object[] params, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsBySqlAsJson(baseDao.getJdbcTemplate(),
                querySql, params, pageDesc);
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql) {
        return JdbcTemplateUtils.listObjectsBySql(baseDao.getJdbcTemplate(),
            querySql, null);
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params) {
        return JdbcTemplateUtils.listObjectsBySql(baseDao.getJdbcTemplate(),
                querySql, params);
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao,
                                                  String querySql, String queryCountSql,PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsBySql(baseDao.getJdbcTemplate(),
            querySql, queryCountSql, null, pageDesc);
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao,
                                                  String querySql, String queryCountSql, Object[] params, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsBySql(baseDao.getJdbcTemplate(),
                querySql, queryCountSql, params, pageDesc);
    }

    public static List<Object[]> listObjectsBySql(BaseDaoImpl<?, ?> baseDao,
                                                  String querySql, Object[] params, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsBySql(baseDao.getJdbcTemplate(),
                querySql, params, pageDesc);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql) {
        return JdbcTemplateUtils.listObjectsBySqlAsJson(baseDao.getJdbcTemplate(),
            querySql, null);
    }

    public static JSONArray listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,  PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsBySqlAsJson(baseDao.getJdbcTemplate(),
            querySql, null, pageDesc);
    }

    public static List<Object[]> listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao,
                                                       String querySql, Map<String, Object> namedParams) {
        return JdbcTemplateUtils.listObjectsByNamedSql(baseDao.getJdbcTemplate(),
                querySql, namedParams);
    }

    public static List<Object[]> listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql,
                                                       Map<String, Object> namedParams, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsByNamedSql(baseDao.getJdbcTemplate(),
                querySql, queryCountSql, namedParams, pageDesc);
    }

    public static List<Object[]> listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                       Map<String, Object> namedParams, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsByNamedSql(baseDao.getJdbcTemplate(),
                querySql, namedParams, pageDesc);
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
                                                        String querySql, String[] fieldNames, String queryCountSql,
                                                        Map<String, Object> namedParams, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsByParamsDriverSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, fieldNames, queryCountSql,
                namedParams, pageDesc);
    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, String[] fieldNames,
                                                   Map<String, Object> namedParams, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsByParamsDriverSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, fieldNames,
                namedParams, pageDesc);
    }

    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, String[] fieldNames,
                                                   Map<String, Object> namedParams) {
        return JdbcTemplateUtils.listObjectsByParamsDriverSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, fieldNames,
                namedParams);
    }


    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao,
                                                   String querySql, String queryCountSql,
                                                   Map<String, Object> namedParams, PageDesc pageDesc) {

        return listObjectsByParamsDriverSqlAsJson(baseDao, querySql,
                null, queryCountSql, namedParams, pageDesc);
    }

    public static JSONArray listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                               Map<String,Object> namedParams) {
        return JdbcTemplateUtils.listObjectsByParamsDriverSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, namedParams);
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
                                            Map<String, Object> namedParams, PageDesc pageDesc) {
        return JdbcTemplateUtils.listObjectsByParamsDriverSqlAsJson(baseDao.getJdbcTemplate(),
                querySql, namedParams, pageDesc);
    }


    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Object[] params, String [] fieldName) {
        return JdbcTemplateUtils.getObjectBySqlAsJson(baseDao.getJdbcTemplate(),
                querySql, params, fieldName);
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Object[] params) {
        return getObjectBySqlAsJson( baseDao, querySql, params, null);
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Map<String, Object> params, String [] fieldName) {
        return JdbcTemplateUtils.getObjectBySqlAsJson(baseDao.getJdbcTemplate(),
                querySql, params, fieldName);
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql,
                                                  Map<String, Object>  params) {
        return getObjectBySqlAsJson( baseDao, querySql, params, null);
    }

    public static JSONObject getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql) {
        return JdbcTemplateUtils.getObjectBySqlAsJson(baseDao.getJdbcTemplate(),querySql);
    }

    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql,
                                                    Map<String,Object> values){
        return JdbcTemplateUtils.getScalarObjectQuery(baseDao.getJdbcTemplate(),
                sSql,values);
    }
    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao,
                                                    String sSql, Object[] values) {
        return JdbcTemplateUtils.getScalarObjectQuery(baseDao.getJdbcTemplate(),
                sSql,values);
    }

    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql)
            throws SQLException, IOException {
        return JdbcTemplateUtils.getScalarObjectQuery(baseDao.getJdbcTemplate(),
                sSql);
    }

    /*
     * * 执行一个标量查询
     */
    public static Object getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sSql,Object value)
            throws SQLException, IOException {
        return JdbcTemplateUtils.getScalarObjectQuery(baseDao.getJdbcTemplate(),
                sSql, value);
    }

    public static Long getSequenceNextValue(BaseDaoImpl<?, ?> baseDao, String sequenceName){
        return JdbcTemplateUtils.getSequenceNextValue(baseDao.getJdbcTemplate(),
                sequenceName);
    }


    /**
     * 保存任意对象
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 保存任意对象数量
     */
    public static int batchSaveNewObjects(BaseDaoImpl<?, ?> baseDao,
                                             Collection<? extends Object> objects) {
        return JdbcTemplateUtils.batchSaveNewObjects(baseDao.getJdbcTemplate(),
                objects);
    }

    /**
     * 更新任意对象
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 更新对象数量
     */
    public static int batchUpdateObjects(BaseDaoImpl<?, ?> baseDao,
                                                Collection<? extends Object> objects) {
        return JdbcTemplateUtils.batchUpdateObjects(baseDao.getJdbcTemplate(),
                objects);
    }


    /**
     * 保存或者更新任意对象 ，每次都先判断是否存在
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return merge对象数量
     */
    public static int batchMergeObjects(BaseDaoImpl<?, ?> baseDao,
                                               Collection<? extends Object> objects) {
        return JdbcTemplateUtils.batchMergeObjects(baseDao.getJdbcTemplate(),
                objects);
    }

    /**
     * 批量删除对象
     * @param baseDao BaseDaoImpl
     * @param objects Collection objects
     * @return 批量删除对象数量
     */
    public static int batchDeleteObjects(BaseDaoImpl<?, ?> baseDao,
                                              Collection<? extends Object> objects) {
        return JdbcTemplateUtils.batchDeleteObjects(baseDao.getJdbcTemplate(),
                objects);
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
    public static <T> Integer batchUpdateObject(BaseDaoImpl<?, ?> baseDao, final Collection<String> fields,
                                         final T object, final Map<String, Object> properties) {
        return JdbcTemplateUtils.batchUpdateObject(baseDao.getJdbcTemplate(),
                fields, object, properties);
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
        return JdbcTemplateUtils.batchUpdateObject(baseDao.getJdbcTemplate(), type,
                propertiesValue, propertiesFilter);
    }

    public static <T> Integer replaceObjectsAsTabulation(
        BaseDaoImpl<?, ?> baseDao, List<T> oldDbObject,
        List<T> newObjects){
        return JdbcTemplateUtils.replaceObjectsAsTabulation
            (baseDao.getJdbcTemplate(), oldDbObject, newObjects);
    }

    public static Integer replaceObjectsAsTabulation(
        BaseDaoImpl<?, ?> baseDao, Class<?> type,
        List<Map<String, Object>> oldDbObject,
        List<Map<String, Object>> newObjects){
        return JdbcTemplateUtils.replaceObjectsAsTabulation
            (baseDao.getJdbcTemplate(), type, oldDbObject, newObjects);
    }
}
