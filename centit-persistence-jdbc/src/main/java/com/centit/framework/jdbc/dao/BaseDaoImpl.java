package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.framework.core.po.EntityWithDeleteTag;
import com.centit.support.database.utils.*;
import com.centit.framework.core.dao.QueryParameterPrepare;
import com.centit.support.algorithm.ListOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmDaoUtils;
import com.centit.support.database.orm.TableMapInfo;
import com.centit.support.file.FileType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unused","unchecked"})
public abstract class BaseDaoImpl<T extends Serializable, PK extends Serializable> {
    protected static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);
    private Class<?> poClass = null;
    private Class<?> pkClass = null;

    protected Map<String, String> filterField = null;
    protected JdbcTemplate jdbcTemplate;

    /**
     * Set the JDBC DataSource to obtain connections from.
     *
     * @param dataSource 数据源
     */
    @Resource
    public void setDataSource(DataSource dataSource) {
        if (this.jdbcTemplate == null || dataSource != this.jdbcTemplate.getDataSource()) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }
    }

    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    public DataSource getDataSource() {
        return (this.jdbcTemplate != null ? this.jdbcTemplate.getDataSource() : null);
    }

    /**
     * Get a JDBC Connection, either from the current transaction or a new one.
     *
     * @return the JDBC Connection
     * @throws CannotGetJdbcConnectionException if the attempt to get a Connection failed
     * @see org.springframework.jdbc.datasource.DataSourceUtils#getConnection(javax.sql.DataSource)
     */
    protected Connection getConnection() throws CannotGetJdbcConnectionException {
        return DataSourceUtils.getConnection(getDataSource());
    }

    /**
     * Close the given JDBC Connection, created via this DAO's DataSource,
     * if it isn't bound to the thread.
     *
     * @param con Connection to close
     * @see org.springframework.jdbc.datasource.DataSourceUtils#releaseConnection
     */
    protected void releaseConnection(Connection con) {
        DataSourceUtils.releaseConnection(con, getDataSource());
    }

    private void fetchTypeParams() {
        ParameterizedType genType = (ParameterizedType) getClass()
                .getGenericSuperclass();
        Type[] params = genType.getActualTypeArguments();
        poClass = ((Class<?>) params[0]);
        pkClass = ((Class<?>) params[1]);
    }

    public Class<?> getPoClass() {
        //return this.getClass().getTypeParameters()[0];
        if (poClass == null) {
            fetchTypeParams();
        }
        return poClass;
    }

    public Class<?> getPkClass() {
        if (pkClass == null) {
            fetchTypeParams();
        }
        return pkClass;
    }

    public String encapsulateFilterToSql(String filterQuery) {
        //QueryUtils.hasOrderBy(filterQuery)
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        if (mapInfo == null)
            throw new PersistenceException(PersistenceException.ORM_METADATA_EXCEPTION,
                    "没有对应的元数据信息：" + getPoClass().getName());

        return "select " + GeneralJsonObjectDao.buildFieldSql(mapInfo, null) +
                " from " + mapInfo.getTableName() +
                " where 1=1 {" + mapInfo.getTableName() + "}" + filterQuery +
                (StringUtils.isBlank(mapInfo.getOrderBy()) ? "" : " order by " + mapInfo.getOrderBy())
                ;
    }

    public String getExtendFilterQuerySql() {
        return ExtendedQueryPool.getExtendedSql(
                FileType.getFileExtName(getPoClass().getName()) + "_QUERY_0");
    }

    public abstract Map<String, String> getFilterField();


    protected static ImmutablePair<String, String> parseParameter(String sParameter) {
        int e = sParameter.indexOf(')');
        if (e > 0) {
            int b = sParameter.indexOf('(') + 1;
            /* b =  b<0 ? 0 :  b+1;*/
            String paramPretreatment = sParameter.substring(b, e).trim();
            String paramAlias = sParameter.substring(e + 1).trim();
            return new ImmutablePair<>(paramAlias, paramPretreatment);
        } else
            return new ImmutablePair<>(sParameter, null);
    }

    public static Map<String, Pair<String, String>>
    getFilterFieldWithPretreatment(Map<String, String> fieldMap) {
        if (fieldMap == null)
            return null;
        Map<String, Pair<String, String>> filterFieldWithPretreatment =
                new HashMap<>(fieldMap.size() * 2);

        for (Map.Entry<String, String> ent : fieldMap.entrySet()) {
            if (StringUtils.isNotBlank(ent.getKey())) {
                ImmutablePair<String, String> paramMeta =
                        parseParameter(ent.getKey());
                filterFieldWithPretreatment.put(paramMeta.left,
                        new ImmutablePair<>(ent.getValue(), paramMeta.getRight()));
            }
        }
        return filterFieldWithPretreatment;
    }

    public static String translatePropertyNameToColumnName(TableMapInfo mapInfo, String sql, String alias) {
        StringBuilder sqlb = new StringBuilder();
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        boolean needTranslate = true;
        int prePos = 0;
        int preWordPos = 0;
        String aWord = lex.getAWord();
        boolean addAlias = StringUtils.isNotBlank(alias);
        //skeep to |
        if ("[".equals(aWord)) {
            aWord = lex.getAWord();
            while (aWord != null && !"".equals(aWord) && !"|".equals(aWord)) {
                if ("(".equals(aWord)) {
                    lex.seekToRightBracket();
                }
                aWord = lex.getAWord();
            }
        }

        while (aWord != null && !"".equals(aWord)) {
            if ("select".equalsIgnoreCase(aWord) || "from".equalsIgnoreCase(aWord)
                  /* || "group".equalsIgnoreCase(aWord) || "order".equalsIgnoreCase(aWord)*/) {
                needTranslate = false;
            } else if ("where".equalsIgnoreCase(aWord)) {
                needTranslate = true;
            }

            if (!needTranslate) {
                preWordPos = lex.getCurrPos();
                aWord = lex.getAWord();
                continue;
            }

            if (":".equals(aWord)) {
                lex.getAWord(); // 跳过参数
                preWordPos = lex.getCurrPos();
                aWord = lex.getAWord();
            }

            if (Lexer.isLabel(aWord)) {
                SimpleTableField col = mapInfo.findFieldByName(aWord);
                if (col != null) {
                    if (preWordPos > prePos)
                        sqlb.append(sql.substring(prePos, preWordPos));
                    sqlb.append(addAlias ? (" " + alias + ".") : " ").append(col.getColumnName());
                    prePos = lex.getCurrPos();
                }
            }
            preWordPos = lex.getCurrPos();
            aWord = lex.getAWord();
        }

        sqlb.append(sql.substring(prePos));

        return sqlb.toString();
    }

    /**
     * 每个dao都需要重载这个函数已获得自定义的查询条件，否则listObjects、pageQuery就等价与listObjectsByProperties
     *
     * @param alias            数据库表别名
     * @param useDefaultFilter 使用默认过滤器
     * @return FilterQuery
     */
    public String buildFieldFilterSql(String alias, boolean useDefaultFilter) {
        StringBuilder sBuilder = new StringBuilder();
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        boolean addAlias = StringUtils.isNotBlank(alias);

        Map<String, Pair<String, String>> fieldFilter =
                getFilterFieldWithPretreatment(getFilterField());

        if (useDefaultFilter || fieldFilter == null || fieldFilter.size() == 0) {//添加默认的过滤条件
            mapInfo.getColumns().stream()
                    .filter(col -> fieldFilter == null || !fieldFilter.containsKey(col.getPropertyName()))
                    .forEach(col ->
                            sBuilder.append(" [:").append(col.getPropertyName()).append("| and ")
                                    .append(col.getColumnName()).append(" = :").append(col.getPropertyName())
                                    .append(" ]"));
        }

        if (fieldFilter != null) {
            for (Map.Entry<String, Pair<String, String>> ent : fieldFilter.entrySet()) {
                String skey = ent.getKey();
                String sSqlFormat = ent.getValue().getLeft();

                if (CodeBook.ORDER_BY_HQL_ID.equalsIgnoreCase(skey))
                    continue;

                if (skey.startsWith(CodeBook.NO_PARAM_FIX)) {
                    sBuilder.append(" [").append(skey).append("| and ")
                            .append(translatePropertyNameToColumnName(mapInfo, sSqlFormat, alias))
                            .append(" ]");
                } else {
                    String pretreatment = ent.getValue().getRight();
                    if (sSqlFormat.equalsIgnoreCase(CodeBook.EQUAL_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(skey);
                        if (col != null) {
                            sBuilder.append(" [:");
                            if (StringUtils.isNotBlank(pretreatment)) {
                                sBuilder.append("(").append(pretreatment).append(")");
                            }
                            sBuilder.append(skey).append("| and ")
                                    .append(addAlias ? (alias + ".") : "")
                                    .append(col.getColumnName()).append(" = :").append(col.getPropertyName())
                                    .append(" ]");
                        }
                    } else if (sSqlFormat.equalsIgnoreCase(CodeBook.LIKE_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(skey);
                        if (col != null) {
                            sBuilder.append(" [:(")
                                    .append(StringUtils.isBlank(pretreatment) ? "like" : pretreatment)
                                    .append(")").append(skey).append("| and ")
                                    .append(addAlias ? (alias + ".") : "")
                                    .append(col.getColumnName()).append(" like :").append(col.getPropertyName())
                                    .append(" ]");
                        }
                    } else if (sSqlFormat.equalsIgnoreCase(CodeBook.IN_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(skey);
                        if (col != null) {
                            sBuilder.append(" [:");
                            if (StringUtils.isNotBlank(pretreatment)) {
                                sBuilder.append("(").append(pretreatment).append(")");
                            }
                            sBuilder.append(skey).append("| and ")
                                    .append(addAlias ? (alias + ".") : "")
                                    .append(col.getColumnName()).append(" in (:").append(col.getPropertyName())
                                    .append(") ]");
                        }
                    } else {
                        if ("[".equals(Lexer.getFirstWord(sSqlFormat))) {
                            sBuilder.append(translatePropertyNameToColumnName(mapInfo, sSqlFormat, alias));
                        } else {
                            sBuilder.append(" [:");
                            if (StringUtils.isNotBlank(pretreatment)) {
                                sBuilder.append("(").append(pretreatment).append(")");
                            }
                            sBuilder.append(skey).append("| and ")
                                    .append(translatePropertyNameToColumnName(mapInfo, sSqlFormat, alias))
                                    .append(" ]");
                        }
                    }
                }// else
            }// for
        }//if(fieldFilter!=null)
        return sBuilder.toString();
    }

    private String daoEmbeddedFilter;

    public String buildDefaultFieldFilterSql() {
        if (daoEmbeddedFilter == null) {
            daoEmbeddedFilter = buildFieldFilterSql(null, false);
        }
        return daoEmbeddedFilter;
    }

    public String getFilterQuerySql() {
        String querySql = getExtendFilterQuerySql();
        if (StringUtils.isBlank(querySql)) {
            querySql = buildDefaultFieldFilterSql();
            return encapsulateFilterToSql(querySql);
        } else {
            if ("[".equals(Lexer.getFirstWord(querySql))) {
                return encapsulateFilterToSql(querySql);
            }
            return querySql;
        }
    }

    public void saveNewObject(T o) {
         /* Integer execute = */
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveNewObject(conn, o));
    }

    public void deleteObjectForce(T o) {
       /* Integer execute = */
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.deleteObject(conn, o));
    }

    public void deleteObjectForceById(Object id) {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.deleteObjectById(conn, id, getPoClass()));
    }

    public void deleteObjectsForceByProperties(Map<String, Object> filterMap) {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.deleteObjectByProperties(conn, filterMap, getPoClass()));
    }

    public void deleteObject(T o) {
      /* Integer execute = */
        if (o instanceof EntityWithDeleteTag) {
            ((EntityWithDeleteTag) o).setDeleted(true);
            this.updateObject(o);
        } else {
            deleteObjectForce(o);
        }
    }

    public void deleteObjectById(Object id) {
        /* Integer execute = */
        if (EntityWithDeleteTag.class.isAssignableFrom(getPoClass())) {
            T o = getObjectById(id);
            ((EntityWithDeleteTag) o).setDeleted(true);
            this.updateObject(o);
        } else {
            deleteObjectForceById(id);
        }
    }

    public void deleteObjectsByProperties(Map<String, Object> filterMap) {
        if (EntityWithDeleteTag.class.isAssignableFrom(getPoClass())) {
            List<T> deleteList = listObjectsByProperties(filterMap);
            if (deleteList != null) {
                for (T obj : deleteList) {
                    ((EntityWithDeleteTag) obj).setDeleted(true);
                    this.updateObject(obj);
                }
            }
        } else {
            deleteObjectsForceByProperties(filterMap);
        }
    }

    public void updateObject(T o) {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn, o));
    }

    public void updateObject(Collection<String> fields, T object)
            throws PersistenceException {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn, fields, object));
    }

    public void batchUpdateObject(Collection<String> fields, T object, Map<String, Object> properties) {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.batchUpdateObject(conn, fields, object, properties));
    }


    public void updateObject(String[] fields, T object)
            throws PersistenceException {
        updateObject(ListOpt.arrayToList(fields), object);
    }

    public void batchUpdateObject(String[] fields, T object, Map<String, Object> properties) {
        batchUpdateObject(ListOpt.arrayToList(fields), object, properties);
    }


    public void mergeObject(T o) {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.mergeObject(conn, o));
    }

    public T getObjectById(Object id) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectById(conn, id, (Class<T>) getPoClass()));
    }

    public T getObjectIncludeLazyById(Object id) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectIncludeLazyById(conn, id, (Class<T>) getPoClass()));
    }

    public T getObjectCascadeById(Object id) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectCascadeById(conn, id, (Class<T>) getPoClass()));
    }

    public T getObjectCascadeShallowById(Object id) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectCascadeShallowById(conn, id, (Class<T>) getPoClass()));
    }

    public T fetchObjectLazyColumn(T o, String columnName) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectLazyColumn(conn, o, columnName));
    }

    public T fetchObjectLazyColumns(T o) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectLazyColumns(conn, o));
    }


    public T fetchObjectReference(T o, String columnName) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectReference(conn, o, columnName));
    }

    public T fetchObjectReferences(T o) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectReferences(conn, o));
    }

    public Integer saveObjectReference(T o, String columnName) {
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveObjectReference(conn, o, columnName));
    }

    public Integer saveObjectReferences(T o) {
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveObjectReferences(conn, o));
    }


    public T getObjectByProperties(Map<String, Object> properties) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectByProperties(conn, properties, (Class<T>) getPoClass()));
    }

    public List<T> listObjectsByProperty(String propertyName, Object value) {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn,
                                QueryUtils.createSqlParamsMap(propertyName, value),
                                (Class<T>) getPoClass()));
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap) {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn, filterMap, (Class<T>) getPoClass()));
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        if (pageDesc != null && pageDesc.getPageSize() > 0 && pageDesc.getPageNo() > 0) {
            return jdbcTemplate.execute(
                    (ConnectionCallback<List<T>>) conn -> {
                        pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn, filterMap, (Class<T>) getPoClass()));
                        return OrmDaoUtils.listObjectsByProperties(conn, filterMap, (Class<T>) getPoClass(),
                                pageDesc.getRowStart(), pageDesc.getPageSize());
                    }
            );
        } else {
            List<T> objList = listObjectsByProperties(filterMap);
            if (pageDesc != null && objList != null) {
                pageDesc.setTotalRows(objList.size());
            }
            return objList;
        }
    }

    /**
     * 这个函数仅仅是为了兼容mybatis版本中的查询
     *
     * @param filterMap 过滤条件
     * @return 总行数
     * 在 JDBC 版本中 请用 listObjectsAsJSON
     */
    @Deprecated
    public int pageCount(Map<String, Object> filterMap) {
        String sql = getFilterQuerySql();
        if (StringUtils.isBlank(sql)) {
            return jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            OrmDaoUtils.fetchObjectsCount(conn, filterMap, (Class<T>) getPoClass()));
        } else {
            QueryAndNamedParams qap = QueryUtils.translateQuery(
                    QueryUtils.buildGetCountSQLByReplaceFields(sql), filterMap);
            return jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            OrmDaoUtils.fetchObjectsCount(conn, qap.getQuery(), qap.getParams()));
        }
    }

    /**
     * 这个函数仅仅是为了兼容mybatis版本中的查询
     *
     * @param filterMap 过滤条件
     * @return 分页数据
     * 在 JDBC 版本中 请用 listObjectsAsJSON
     */
    @Deprecated
    public List<T> pageQuery(Map<String, Object> filterMap) {
        String querySql = getFilterQuerySql();
        PageDesc pageDesc = QueryParameterPrepare.fetckPageDescParams(filterMap);
        if (StringUtils.isBlank(querySql)) {
            return listObjectsByProperties(filterMap, pageDesc);
        } else {
            String selfOrderBy = fetchSelfOrderSql(querySql, filterMap);
            if (StringUtils.isNotBlank(selfOrderBy)) {
                querySql = QueryUtils.removeOrderBy(querySql) + " order by " + selfOrderBy;
            }
            QueryAndNamedParams qap = QueryUtils.translateQuery(querySql, filterMap);
            return jdbcTemplate.execute(
                    /** 这个地方可以用replaceField 已提高效率
                     *  pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn,
                     QueryUtils.buildGetCountSQLByReplaceFields(qap.getSql()),qap.getParams()));
                     * */
                    (ConnectionCallback<List<T>>) conn -> OrmDaoUtils
                            .queryObjectsByNamedParamsSql(conn, qap.getQuery(), qap.getParams(), (Class<T>) getPoClass(),
                                    pageDesc.getRowStart(), pageDesc.getPageSize()));
        }
    }

    /**
     * 查询所有数据
     *
     * @return 返回所有数据 listAllObjects
     */
    public List<T> listObjects() {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listAllObjects(conn, (Class<T>) getPoClass()));
    }

    /**
     * 根据设定的条件查询数据对象
     *
     * @param filterMap 过滤条件
     * @return 返回符合条件的对象
     */
    public List<T> listObjects(Map<String, Object> filterMap) {
        String querySql = getFilterQuerySql();
        if (StringUtils.isBlank(querySql)) {
            return listObjectsByProperties(filterMap);
        } else {
            String selfOrderBy = fetchSelfOrderSql(querySql, filterMap);
            if (StringUtils.isNotBlank(selfOrderBy)) {
                querySql = QueryUtils.removeOrderBy(querySql) + " order by " + selfOrderBy;
            }
            QueryAndNamedParams qap = QueryUtils.translateQuery(querySql, filterMap);
            return jdbcTemplate.execute(
                    (ConnectionCallback<List<T>>) conn ->
                            OrmDaoUtils.queryObjectsByNamedParamsSql(conn, qap.getQuery(), qap.getParams(), (Class<T>) getPoClass())
            );
        }
    }

    /**
     * 根据条件查询对象
     *
     * @param whereSql 只有 where 部分， 不能有from部分 这个式hibernate的区别
     * @param params   参数
     * @return 符合条件的对象
     */
    public List<T> listObjectsByFilter(String whereSql, Object[] params) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        String fieldsSql = GeneralJsonObjectDao.buildFieldSql(mapInfo, null);
        String querySql = "select " + fieldsSql + " from " + mapInfo.getTableName()
                + " " + whereSql;

        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.queryObjectsByParamsSql(conn, querySql, params, (Class<T>) getPoClass()));
    }

    /**
     * 根据条件查询对象
     *
     * @param whereSql    只有 where 部分， 不能有from部分 这个式hibernate的区别
     * @param namedParams 命名参数
     * @return 符合条件的对象
     */
    public List<T> listObjectsByFilter(String whereSql, Map<String, Object> namedParams) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        String fieldsSql = GeneralJsonObjectDao.buildFieldSql(mapInfo, null);
        String querySql = "select " + fieldsSql + " from " + mapInfo.getTableName()
                + " " + whereSql;

        return listObjectsBySql(querySql, namedParams);
    }

    /**
     * 由于性能问题，不推荐使用这个方法，分页查询一般都是用于前端展示的，建议使用  listObjectsByFilterAsJson
     *
     * @param whereSql 查询po 所以只有套写 where 以后部分
     * @param params   查询参数
     * @param pageDesc 分页信息
     * @return 返回对象
     */
    @Deprecated
    public List<T> listObjectsByFilter(String whereSql, Object[] params, PageDesc pageDesc) {
        if (pageDesc != null && pageDesc.getPageSize() > 0 && pageDesc.getPageNo() > 0) {

            TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
            String fieldsSql = GeneralJsonObjectDao.buildFieldSql(mapInfo, null);
            String querySql = "select " + fieldsSql + " from " + mapInfo.getTableName()
                    + " " + whereSql;

            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                    DatabaseOptUtils.getScalarObjectQuery(this,
                            "select count(1) from " +
                                    mapInfo.getTableName() + " " + QueryUtils.removeOrderBy(whereSql),
                            params))
            );

            return jdbcTemplate.execute(
                    (ConnectionCallback<List<T>>) conn ->
                            OrmDaoUtils.queryObjectsByParamsSql(
                                    conn, querySql, params, (Class<T>) getPoClass(),
                                    pageDesc.getRowStart(), pageDesc.getPageSize()));
        } else {
            List<T> objList = listObjectsByFilter(whereSql, params);
            if (pageDesc != null && objList != null) {
                pageDesc.setTotalRows(objList.size());
            }
            return objList;
        }
    }

    /**
     * 由于性能问题，不推荐使用这个方法，分页查询一般都是用于前端展示的，建议使用  listObjectsByFilterAsJson
     *
     * @param whereSql    查询po 所以只有套写 where 以后部分
     * @param namedParams 查询参数
     * @param pageDesc    分页信息
     * @return 返回对象
     */
    @Deprecated
    public List<T> listObjectsByFilter(String whereSql, Map<String, Object> namedParams, PageDesc pageDesc) {

        if (pageDesc != null && pageDesc.getPageSize() > 0 && pageDesc.getPageNo() > 0) {
            TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
            String fieldsSql = GeneralJsonObjectDao.buildFieldSql(mapInfo, null);
            String querySql = "select " + fieldsSql + " from " + mapInfo.getTableName()
                    + " " + whereSql;
            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                    DatabaseOptUtils.getScalarObjectQuery(this, "select count(1) from " +
                            mapInfo.getTableName() + " " + QueryUtils.removeOrderBy(whereSql), namedParams))
            );
            return jdbcTemplate.execute(
                    (ConnectionCallback<List<T>>) conn ->
                            OrmDaoUtils.queryObjectsByNamedParamsSql(
                                    conn, querySql, namedParams, (Class<T>) getPoClass(),
                                    pageDesc.getRowStart(), pageDesc.getPageSize()));
        } else {
            List<T> objList = listObjectsByFilter(whereSql, namedParams);
            if (pageDesc != null && objList != null) {
                pageDesc.setTotalRows(objList.size());
            }
            return objList;
        }
    }

    public List<T> listObjectsBySql(String querySql, Map<String, Object> namedParams) {

        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.queryObjectsByNamedParamsSql(
                                conn, querySql, namedParams, (Class<T>) getPoClass()));

    }

    public static String fetchSelfOrderSql(String querySql, Map<String, Object> filterMap) {
        String selfOrderBy = StringBaseOpt.objectToString(filterMap.get(CodeBook.SELF_ORDER_BY));
        if (StringUtils.isBlank(selfOrderBy)) {
            String sortField = StringBaseOpt.objectToString(filterMap.get(CodeBook.TABLE_SORT_FIELD));
            if (StringUtils.isNotBlank(sortField)) {
                sortField = DatabaseOptUtils.mapFieldToColumnPiece(querySql, sortField);
                if (sortField != null) {
                    selfOrderBy = sortField;
                    String sOrder = StringBaseOpt.objectToString(filterMap.get(CodeBook.TABLE_SORT_ORDER));
                    if (/*"asc".equalsIgnoreCase(sOrder) ||*/ "desc".equalsIgnoreCase(sOrder)) {
                        selfOrderBy = sortField + " desc";
                    }
                }
            }
        }
        return selfOrderBy;
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param filterMap 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param filters 数据权限顾虑语句
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsAsJson(Map<String, Object> filterMap, Collection<String> filters, PageDesc pageDesc) {

        String querySql = getFilterQuerySql();

        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Pair<String, String[]> q = GeneralJsonObjectDao.buildFieldSqlWithFieldName(mapInfo, null);

        String selfOrderBy = fetchSelfOrderSql(querySql, filterMap);

        if (StringUtils.isNotBlank(selfOrderBy)) {
            querySql = QueryUtils.removeOrderBy(querySql) + " order by " + selfOrderBy;
        }

        QueryAndNamedParams qap = QueryUtils.translateQuery(querySql, filters, filterMap, false);

        if (pageDesc != null && pageDesc.getPageSize() > 0) {
            return DatabaseOptUtils.listObjectsBySqlAsJson(this, qap.getQuery(), q.getRight(),
                    QueryUtils.buildGetCountSQLByReplaceFields(qap.getQuery()), qap.getParams(), pageDesc);
        } else {
            return DatabaseOptUtils.listObjectsBySqlAsJson(this, qap.getQuery(), q.getRight(), qap.getParams());
        }
    }

    public JSONArray listObjectsAsJson(Map<String, Object> filterMap, PageDesc pageDesc){
        return listObjectsAsJson(filterMap, null, pageDesc);
    }


    /**
     *
     * @param whereSql 查询po 所以只有套写 where 以后部分
     * @param namedParams 查询参数
     * @param pageDesc 分页信息
     * @return 返回JSONArray
     */
    public JSONArray listObjectsByFilterAsJson(String whereSql,  Map<String, Object> namedParams, PageDesc pageDesc){
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Pair<String,String[]>  fieldsDesc = GeneralJsonObjectDao.buildFieldSqlWithFieldName(mapInfo,null);
        String querySql = "select " + fieldsDesc.getLeft() +" from " +mapInfo.getTableName()
                + " " +whereSql;

        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return DatabaseOptUtils.listObjectsBySqlAsJson(this, querySql, fieldsDesc.getRight() ,
                    QueryUtils.buildGetCountSQLByReplaceFields( querySql ), namedParams,   pageDesc  );
        }else{
            return DatabaseOptUtils.listObjectsBySqlAsJson(this, querySql, fieldsDesc.getRight(), namedParams);
        }
    }

    /**
     *
     * @param whereSql 查询po 所以只有套写 where 以后部分
     * @param params 查询参数
     * @param pageDesc 分页信息
     * @return 返回JSONArray
     */
    public JSONArray listObjectsByFilterAsJson(String whereSql,  Object[] params, PageDesc pageDesc){
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Pair<String,String[]>  fieldsDesc = GeneralJsonObjectDao.buildFieldSqlWithFieldName(mapInfo,null);
        String querySql = "select " + fieldsDesc.getLeft() +" from " +mapInfo.getTableName()
                + " " +whereSql;

        if(pageDesc!=null && pageDesc.getPageSize()>0) {
            return DatabaseOptUtils.listObjectsBySqlAsJson(this, querySql, fieldsDesc.getRight() ,
                    QueryUtils.buildGetCountSQLByReplaceFields( querySql ), params,   pageDesc  );
        }else{
            return DatabaseOptUtils.listObjectsBySqlAsJson(this, querySql, params, fieldsDesc.getRight());
        }
    }

}
