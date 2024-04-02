package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.core.po.EntityWithDeleteTag;
import com.centit.framework.core.po.EntityWithVersionTag;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.common.LeftRightPair;
import com.centit.support.common.ObjectException;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableReference;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmDaoUtils;
import com.centit.support.database.orm.OrmUtils;
import com.centit.support.database.orm.TableMapInfo;
import com.centit.support.database.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * 针对 EntityWithDeleteTag EntityWithVersionTag 这个jpa类只做了一个浅层实现。
 * 如果需要循环处理引用可以调用 OrmDaoUtils.*Cascade*()相关的方法 自行实现
 * @see OrmDaoUtils
 * @param <T> po类
 * @param <PK> po主键类型 ; 对多个字段联合主键的可以使用Map《String, Object》类型
 */
@SuppressWarnings({"unused","unchecked"})
public abstract class BaseDaoImpl<T extends Serializable, PK extends Serializable> {
    protected static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);
    private Class<?> poClass = null;
    private Class<?> pkClass = null;

    /*
     * 保留和以前版本的兼容，下一个版本将删除
     */
    //@Deprecated
    //protected Map<String, String> filterField = null;

    protected JdbcTemplate jdbcTemplate;
    private static final int DEFAULT_CASCADE_DEPTH = 3;
    /**
     * Set the JDBC DataSource to obtain connections from.
     * @param dataSource 数据源
     */
    @Resource
    public void setDataSource(DataSource dataSource) {
        if (this.jdbcTemplate == null || dataSource != this.jdbcTemplate.getDataSource()) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }
    }

    /**
     * 获取spring jdbc 的 jdbcTemplate
     * @return JdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    /**
     * 获取数据源 这个一般不要使用
     * @return DataSource
     */
    public DataSource getDataSource() {
        return (this.jdbcTemplate != null ? this.jdbcTemplate.getDataSource() : null);
    }

    /**
     * Get a JDBC Connection, either from the current transaction or a new one.
     * 请不要使用这个方法，我们一般获取jdbcTemplate来操作数据库
     * @return the JDBC Connection
     * @throws CannotGetJdbcConnectionException if the attempt to get a Connection failed
     * @see org.springframework.jdbc.datasource.DataSourceUtils#getConnection(javax.sql.DataSource)
     */
    @Deprecated
    public Connection getConnection() throws CannotGetJdbcConnectionException {
        return DataSourceUtils.getConnection(getDataSource());
    }

    public DBType getDBtype() {
        return jdbcTemplate.execute(
            (ConnectionCallback<DBType>) conn -> DBType.mapDBType(conn)
        );
    }

    /**
     * Close the given JDBC Connection, created via this DAO's DataSource,
     * if it isn't bound to the thread.
     *
     * @param con Connection to close
     * @see org.springframework.jdbc.datasource.DataSourceUtils#releaseConnection
     */
    @Deprecated
    public void releaseConnection(Connection con) {
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

    public String encapsulateFilterToSql(String fieldsSql, String filterQuery,
                                         String tableAlias, String orderBySql, boolean withExtFilter) {
        boolean addAlias = StringUtils.isNotBlank(tableAlias);
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        StringBuilder sqlBuilder = new StringBuilder("select ");
        sqlBuilder.append(fieldsSql).append(" from ")
            .append(mapInfo.getTableName());
        if(addAlias){
            sqlBuilder.append(" ").append(tableAlias);
        }
        sqlBuilder.append(" where 1=1 ");
        if(withExtFilter){
            sqlBuilder.append("{").append(mapInfo.getTableName());
            if (addAlias) {
                sqlBuilder.append(" ").append(tableAlias);
            }
            sqlBuilder.append(" }");
        }
        if(StringUtils.isNotBlank(filterQuery)){
            if(StringUtils.equalsAnyIgnoreCase(
                Lexer.getFirstWord(filterQuery),"[", "and", "or")) {
                sqlBuilder.append(filterQuery);
            }else{
                sqlBuilder.append(" and ").append(filterQuery);
            }
        }
        if(StringUtils.isNotBlank(orderBySql)){
            sqlBuilder.append(" order by " ).append(orderBySql);
        }
        return sqlBuilder.toString();
    }

    public String encapsulateFilterToFields(Collection<String> fields, String filterQuery,
                                            String tableAlias, boolean withExtFilter) {
        //QueryUtils.hasOrderBy(filterQuery)
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        String fieldsSql =
                ((fields != null && fields.size()>0)
                    ? GeneralJsonObjectDao.buildPartFieldSql(mapInfo, fields, tableAlias, true)
                    : GeneralJsonObjectDao.buildFieldSql(mapInfo, tableAlias, 1));
        return encapsulateFilterToSql(fieldsSql, filterQuery, tableAlias, mapInfo.getOrderBy(), withExtFilter);
    }

    private Map<String, DataFilter> insideFieldFilter = null;
    /**
     * 每个dao都要初始化filterField这个对象，在 getFilterField 初始化，并且返回
     * @return 返回 getFilterField 属性
     */
    public Map<String, String> getFilterField(){
        return null;
    }

    /**
     * 每个dao都需要重载这个函数已获得自定义的查询条件，否则listObjects、pageQuery就等价与listObjectsByProperties
     * 根据 getFilterField 中的内容初始化
     * @param mapInfo 表机构元数据
     * @return FilterQuery
     */
    public Map<String, DataFilter> obtainInsideFilters(TableMapInfo mapInfo){
        if(insideFieldFilter==null) {
            insideFieldFilter = new HashMap<>();
            Map<String, String> filters = getFilterField();
            if(filters!=null && filters.size()>0){
                for(Map.Entry<String, String> ent : filters.entrySet()) {
                    DataFilter dataFilter = new DataFilter(ent.getKey(), ent.getValue());

                    if (dataFilter.getFilterSql().equalsIgnoreCase(CodeBook.EQUAL_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(dataFilter.getFormule());
                        if (col != null) {
                            dataFilter.setFilterSql(col.getColumnName() + " = :" + dataFilter.getValueName());
                            insideFieldFilter.put(dataFilter.getFormule() ,dataFilter);
                        }
                    } else if (dataFilter.getFilterSql().equalsIgnoreCase(CodeBook.LIKE_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(dataFilter.getFormule());
                        if (col != null) {
                            dataFilter.setFilterSql(col.getColumnName() + " like :" + dataFilter.getValueName() );
                            insideFieldFilter.put(dataFilter.getFormule() ,dataFilter);
                        }
                    } else if (dataFilter.getFilterSql().equalsIgnoreCase(CodeBook.IN_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(dataFilter.getFormule());
                        if (col != null) {
                            dataFilter.setFilterSql(col.getColumnName() + " in (:" + dataFilter.getValueName() +")" );
                            insideFieldFilter.put(dataFilter.getFormule() ,dataFilter);
                        }
                    } else {
                        dataFilter.setFilterSql(
                            JpaMetadata.translateSqlPropertyToColumn(mapInfo, dataFilter.getFilterSql(), null));
                        insideFieldFilter.put(dataFilter.getFormule() ,dataFilter);
                    }
                }// for
            }
        }
        return insideFieldFilter;
    }
    public String fetchSelfOrderSql(Map<String, Object> filterMap) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        String selfOrderBy = GeneralJsonObjectDao.fetchSelfOrderSql(mapInfo, filterMap);
        if(StringUtils.equals(selfOrderBy,  mapInfo.getOrderBy())) {
            Map<String, DataFilter> filterList = obtainInsideFilters(mapInfo);
            DataFilter df = filterList.get(CodeBook.SELF_ORDER_BY);
            if (df != null) {
                return df.getFilterSql();
            }
        }
        return selfOrderBy;
    }

    public LeftRightPair<QueryAndNamedParams, TableField[]> buildQueryByParamsWithFields(Map<String, Object> filterMap, Collection<String> fields,
                                                                             Collection<String> extentFilters, QueryUtils.IFilterTranslater powerTranslater){

        String selfOrderBy = fetchSelfOrderSql(filterMap);

        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());

        Pair<String, TableField[]> q = ((fields != null && fields.size()>0)
            ? GeneralJsonObjectDao.buildPartFieldSqlWithFields(mapInfo, fields, null, true)
            : GeneralJsonObjectDao.buildFieldSqlWithFields(mapInfo, null, true));

        QueryAndNamedParams queryAndParams = buildFilterByParams(filterMap, extentFilters, powerTranslater);
        String querySql = encapsulateFilterToSql(q.getLeft(), queryAndParams.getQuery(), null, selfOrderBy, false);
        queryAndParams.setQuery(querySql);
        return new LeftRightPair<>(queryAndParams, q.getRight());
    }

    protected QueryAndNamedParams buildQueryByParams(Map<String, Object> filterMap, Collection<String> fields,
                                         Collection<String> extentFilters, QueryUtils.IFilterTranslater powerTranslater) {
        String selfOrderBy = fetchSelfOrderSql(filterMap);

        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());

        String sql = ((fields != null && fields.size()>0)
            ? GeneralJsonObjectDao.buildPartFieldSql(mapInfo, fields, null, true)
            : GeneralJsonObjectDao.buildFieldSql(mapInfo, null, 1));

        QueryAndNamedParams queryAndParams = buildFilterByParams(filterMap, extentFilters, powerTranslater);
        String querySql = encapsulateFilterToSql(sql, queryAndParams.getQuery(), null, selfOrderBy, false);
        queryAndParams.setQuery(querySql);
        return queryAndParams;
    }

    protected QueryAndNamedParams buildFilterByParams(Map<String, Object> filterMap,
                                                      Collection<String> extentFilters, QueryUtils.IFilterTranslater powerTranslater) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Map<String, Object> queryParams = new HashMap<>(filterMap.size()+4);
        Map<String, DataFilter> filterList = obtainInsideFilters(mapInfo);
        Map<String, Object> leftFilterMap ;
        Map<String, LeftRightPair<Integer , StringBuilder>> filterGroup =
            new HashMap<>();
        //buildFilterSqlPieces(ti, alias,  filterMap, filterGroup);

        if(!filterList.isEmpty()){
            leftFilterMap = new HashMap<>(filterMap.size()+4);
            for(Map.Entry<String, Object> ent : filterMap.entrySet()) {
                if(ent.getValue()==null)
                    continue;
                DataFilter df = filterList.get(ent.getKey());
                if(df != null){
                    String plCol = df.getValueName();
                    String groupName="nog";
                    boolean haveGroupSign= plCol.length()>3 && (plCol.charAt(0) == 'g' || plCol.charAt(0) == 'G') &&
                        plCol.charAt(1) >= '0' && plCol.charAt(1) <= '9' &&
                        (plCol.charAt(2) == '_' || plCol.charAt(3) == '_');
                    if(haveGroupSign){
                        groupName = "g" + plCol.charAt(1);
                    }
                    LeftRightPair<Integer , StringBuilder> groupFilter = filterGroup.get(groupName);
                    StringBuilder currentBuild;
                    if(groupFilter == null){
                        currentBuild = new StringBuilder();
                        groupFilter = new LeftRightPair<>(1, currentBuild);
                        filterGroup.put(groupName, groupFilter);
                    } else {
                        groupFilter.setLeft( groupFilter.getLeft() +1);
                        currentBuild = groupFilter.getRight();
                        currentBuild.append(haveGroupSign? " or " : " and ");
                    }

                    queryParams.put(df.getValueName(), QueryUtils.pretreatParameter(df.getPretreatment(),ent.getValue() ));
                    currentBuild.append(df.getFilterSql());
                } else {
                    leftFilterMap.put(ent.getKey(), ent.getValue());
                }
            }
        } else {
            leftFilterMap = filterMap;
        }

        if(leftFilterMap.size()>0) {
            GeneralJsonObjectDao.buildFilterSqlPieces(mapInfo, null, leftFilterMap, filterGroup);
            queryParams.putAll(leftFilterMap);
        }
        StringBuilder filterQuery = new StringBuilder();
        //外部条件，一般是权限引擎的表达式
        if(extentFilters!=null && extentFilters.size()>0) {
            QueryUtils.IFilterTranslater translater = powerTranslater !=null ?
                powerTranslater : new QueryUtils.SimpleFilterTranslater(filterMap);
            Map<String, String> tableAlias = new HashMap<>(2);
            tableAlias.put(mapInfo.getTableName(), "");
            translater.setTableAlias(tableAlias );
            QueryAndNamedParams powerFilter = QueryUtils.translateQueryFilter(extentFilters, translater, true);
            if(powerFilter != null && StringUtils.isNotBlank(powerFilter.getQuery())){
                filterQuery.append(" and ").append(powerFilter.getQuery());
                queryParams.putAll(powerFilter.getParams());
            }
        }

        for(Map.Entry<String, LeftRightPair<Integer , StringBuilder>> ent : filterGroup.entrySet()){
            filterQuery.append(" and ");
            if(ent.getValue().getLeft() > 1 && !"nog".equals(ent.getKey()))
                filterQuery.append("(").append(ent.getValue().getRight().toString()).append(")");
            else
                filterQuery.append(ent.getValue().getRight().toString());
        }

        return new QueryAndNamedParams(filterQuery.toString(), queryParams);
    }

    private void innerSaveNewObject(Object o) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(o.getClass());

        if (o instanceof EntityWithVersionTag) {
            EntityWithVersionTag ewvto = (EntityWithVersionTag) o;
            SimpleTableField field = mapInfo.findFieldByColumn(ewvto.obtainVersionProperty());
            Object obj = mapInfo.getObjectFieldValue(o, field);
            if(obj == null){
                mapInfo.setObjectFieldValue(o, field, ewvto.calcNextVersion());
            }
        }
        // 检测是否有自动增长主键
        if(mapInfo.hasGeneratedKeys()){
            Map<String, Object> ids = jdbcTemplate.execute(
                (ConnectionCallback<Map<String, Object>>) conn ->
                    OrmDaoUtils.saveNewObjectAndFetchGeneratedKeys(conn, o));
            //写回主键值 一个表中只能有一个自增ID
            if(ids !=null && !ids.isEmpty()) {
                SimpleTableField filed = mapInfo.fetchGeneratedKey();
                if (filed != null){
                    mapInfo.setObjectFieldValue(o, filed, ids.values().iterator().next());
                }
            }
        } else {
            jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                    OrmDaoUtils.saveNewObject(conn, o));
        }
    }

    public void saveNewObject(T o) {
        innerSaveNewObject(o);
    }

    private int deleteObjectWithVersion(final Object o){
        return jdbcTemplate.execute(
            (ConnectionCallback<Integer>) conn -> {
                TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(o.getClass());
                JsonObjectDao sqlDialect = GeneralJsonObjectDao.createJsonObjectDao(conn, mapInfo);

                EntityWithVersionTag ewvto = (EntityWithVersionTag) o;
                SimpleTableField field = mapInfo.findFieldByColumn(ewvto.obtainVersionProperty());
                //获取旧版本
                Object oleVsersion = mapInfo.getObjectFieldValue(o, field);

                Map<String, Object> idMap = OrmUtils.fetchObjectDatabaseField(o, mapInfo);
                if (!GeneralJsonObjectDao.checkHasAllPkColumns(mapInfo, idMap)) {
                    throw new SQLException("缺少主键对应的属性。");
                }
                String sql ="delete from " + mapInfo.getTableName() +
                        " where " + GeneralJsonObjectDao.buildFilterSqlByPk(mapInfo, null) +
                        " and " + field.getColumnName() + " = :_oldVersion";
                idMap.put("_oldVersion", oleVsersion);
                return DatabaseAccess.doExecuteNamedSql(conn, sql, idMap);
            });
    }

    private int innerDeleteObjectForce(Object o) {
        if (o instanceof EntityWithVersionTag) {
            return deleteObjectWithVersion(o);
        } else {
            /* Integer execute = */
            return jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            OrmDaoUtils.deleteObject(conn, o));
        }
    }

    public int deleteObjectForce(T o) {
        return innerDeleteObjectForce(o);
    }

    public int deleteObjectForceById(Object id) {
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.deleteObjectById(conn, id, getPoClass()));
    }

    private int innerDeleteObject(Object o) {
      /* Integer execute = */
        if (o instanceof EntityWithDeleteTag) {
            ((EntityWithDeleteTag) o).setDeleted(true);
            return this.innerUpdateObject(o);
        } else {
            return this.innerDeleteObjectForce(o);
        }
    }

    public int deleteObject(T o) {
        return innerDeleteObject(o);
    }

    public int deleteObjectById(Object id) {
        /* Integer execute = */
        T o = getObjectById(id);
        if(o != null) {
            return innerDeleteObject(o);
        }
        return 0;
    }

    private int updateObjectWithVersion(final Object o, Collection<String> fields){
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn -> {
                    TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(o.getClass());
                    JsonObjectDao sqlDialect = GeneralJsonObjectDao.createJsonObjectDao(conn, mapInfo);
                    try {
                        OrmUtils.prepareObjectForUpdate(o, mapInfo, sqlDialect);
                    } catch (IOException e) {
                        throw new ObjectException(e);
                    }
                    EntityWithVersionTag ewvto = (EntityWithVersionTag) o;
                    SimpleTableField field = mapInfo.findFieldByColumn(ewvto.obtainVersionProperty());
                    //获取旧版本
                    Object oleVsersion = mapInfo.getObjectFieldValue(o, field);
                    //设置新版本
                    mapInfo.setObjectFieldValue(o, field, ewvto.calcNextVersion());
                    Map<String, Object> objMap = OrmUtils.fetchObjectDatabaseField(o, mapInfo);

                    if (!GeneralJsonObjectDao.checkHasAllPkColumns(mapInfo, objMap)) {
                        throw new SQLException("缺少主键对应的属性。");
                    }
                    String sql = GeneralJsonObjectDao.buildUpdateSql(mapInfo,
                            fields==null? objMap.keySet():fields);
                    if(sql==null) { // 没有需要更新的内容
                        return 0;
                    }
                    sql = sql + " where " + GeneralJsonObjectDao.buildFilterSqlByPk(mapInfo, null) +
                            " and " + field.getColumnName() + " = :_oldVersion";
                    objMap.put("_oldVersion", oleVsersion);
                    return DatabaseAccess.doExecuteNamedSql(conn, sql, objMap);
                });
    }

    private int innerUpdateObject(final Object o) {
        if (o instanceof EntityWithVersionTag) {
            return updateObjectWithVersion(o, null);
        }
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn, o));
    }

    public int updateObject(final T o) {
        return innerUpdateObject(o);
    }
    /**
     * 只更改对象的部分属性
     * @param fields 需要修改的部分属性
     * @param object 除了对应修改的属性 需要有相应的值，主键对应的属性也必须要值
     * @throws ObjectException 运行时异常
     * @return 是否 更新了数据库
     */
    public int updateObject(Collection<String> fields, T object)
            throws ObjectException {
        if (object instanceof EntityWithVersionTag) {
            return updateObjectWithVersion(object, fields);
        }
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn, fields, object));
    }

    /**
     * 只更改对象的部分属性
     * @param fields 需要修改的部分属性
     * @param object 除了对应修改的属性 需要有相应的值，主键对应的属性也必须要值
     * @throws ObjectException 运行时异常
     * @return 是否 更新了数据库
     */
    public int updateObject(String[] fields, T object)
            throws ObjectException {
        if (object instanceof EntityWithVersionTag) {
            return updateObjectWithVersion(object, CollectionsOpt.arrayToList(fields));
        }
        return  updateObject(CollectionsOpt.arrayToList(fields), object);
    }

    public int updateObjectWithNullField(T object, boolean includeLazy)
        throws ObjectException {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        List<String> fields = includeLazy ? mapInfo.getAllFieldsName()
                  : mapInfo.getFieldsNameWithoutLazy();
        return updateObject(fields, object);
    }

    public int updateObjectWithNullField(T object)
        throws ObjectException {
        return updateObjectWithNullField(object, false);
    }

    public int mergeObject(T o) {
        if (o instanceof EntityWithVersionTag) {
            T dbObj = this.getObjectById(o);
            if(dbObj==null){
                this.innerSaveNewObject(o);
                return 1;
            }else{
                return this.innerUpdateObject(o);
            }
        }
        return  jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.mergeObject(conn, o));
    }

    public T getObjectById(Object id) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectById(conn, id, (Class<T>) getPoClass()));
    }

    public T getObjectExcludeLazyById(Object id) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectExcludeLazyById(conn, id, (Class<T>) getPoClass()));
    }

    public T getObjectWithReferences(Object id) {
        T obj = getObjectById(id);
        if(obj==null){
            return null;
        }
        return fetchObjectReferences(obj);
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

    private List<?> innerFetchObjectReference(T object, SimpleTableReference ref ){
        if(object==null || ref==null || ref.getReferenceColumns().size()<1)
            return null;

        Class<?> refType = ref.getTargetEntityType();
        TableMapInfo refMapInfo = JpaMetadata.fetchTableMapInfo(refType );
        if( refMapInfo == null )
            return null;
        Map<String, Object> properties = ref.fetchChildFk(object);

        return jdbcTemplate.execute(
                (ConnectionCallback<List<?>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn, properties, refType));
    }

    public T fetchObjectReference(T object, String columnName) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        SimpleTableReference ref = mapInfo.findReference(columnName);
        Class<?> refType = ref.getTargetEntityType();
        List<?> refs = innerFetchObjectReference(object, ref);

        if(refs!=null && refs.size()>0) {
            if (//ref.getReferenceFieldType().equals(refType) /*||
                    ref.getReferenceFieldType().isAssignableFrom(refType) ){
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    for(Object refObject : refs)
                    if( ! ((EntityWithDeleteTag)refObject).isDeleted()){
                        ref.setObjectFieldValue(object,refObject);
                        break;
                    }
                } else {
                    ref.setObjectFieldValue(object, refs.get(0));
                }
            } else if(Set.class.isAssignableFrom(ref.getReferenceFieldType())){
                Set<Object> validRefDate = new HashSet<>(refs.size()+1);
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    for(Object refObject : refs)
                        if( ! ((EntityWithDeleteTag)refObject).isDeleted()){
                            validRefDate.add(refObject);
                        }
                } else {
                    validRefDate.addAll(refs);
                }
                ref.setObjectFieldValue(object,validRefDate);
            }else if(List.class.isAssignableFrom(ref.getReferenceFieldType())){
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    List<Object>  validRefDate = new ArrayList<>(refs.size());
                    for(Object refObject : refs) {
                        if (!((EntityWithDeleteTag) refObject).isDeleted()) {
                            validRefDate.add(refObject);
                        }
                    }
                    ref.setObjectFieldValue(object, validRefDate);
                } else {
                    ref.setObjectFieldValue(object, refs);
                }
            }
        }
        return object;
    }

    public T fetchObjectReferences(T o) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        if(mapInfo.hasReferences()) {
            for (SimpleTableReference ref : mapInfo.getReferences()) {
                fetchObjectReference(o, ref.getReferenceName());
            }
        }
        return o;
    }

    public int deleteObjectReference(T object, String columnName) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        SimpleTableReference ref = mapInfo.findReference(columnName);

        List<?> refs = innerFetchObjectReference(object, ref);

        if(refs!=null && refs.size()>0){
            for(Object refObject : refs) {
                innerDeleteObject(refObject);
            }
        }
        return 1;
    }

    public int deleteObjectReferences(T object) {
        int nRes = 0;
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo( getPoClass());
        if(mapInfo.hasReferences()) {
            for (SimpleTableReference ref : mapInfo.getReferences()) {
                nRes += deleteObjectReference(object, ref.getReferenceName());
            }
        }
        return nRes;
    }

    public int deleteObjectReferenceForce(T object, String columnName) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        SimpleTableReference ref = mapInfo.findReference(columnName);
        List<?> refs = innerFetchObjectReference(object, ref);

        if(refs!=null && refs.size()>0){
            for(Object refObject : refs) {
                innerDeleteObjectForce(refObject);
            }
        }
        return 1;
    }

    public int deleteObjectReferencesForce(T object) {
        int nRes = 0;
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo( getPoClass());
        if(mapInfo.hasReferences()) {
            for (SimpleTableReference ref : mapInfo.getReferences()) {
                nRes += deleteObjectReferenceForce(object, ref.getReferenceName());
            }
        }
        return nRes;
    }

    public int saveObjectReference(T object, String columnName) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        SimpleTableReference ref = mapInfo.findReference(columnName);
        if(ref==null || ref.getReferenceColumns().size()<1)
            return 0;

        Class<?> refType = ref.getTargetEntityType();
        TableMapInfo refMapInfo = JpaMetadata.fetchTableMapInfo( refType );
        if( refMapInfo == null )
            return 0;

        List<?> refs = innerFetchObjectReference(object, ref);
        Object newObj = ref.getObjectFieldValue(object);
        if(newObj == null){ // delete all
            if(refs!=null && refs.size()>0){
                for(Object refObject : refs) {
                    innerDeleteObject(refObject);
                }
            }
            return 1;
        }

        OrmDaoUtils.OrmObjectComparator refObjComparator = new OrmDaoUtils.OrmObjectComparator(refMapInfo);
        if (//ref.getReferenceFieldType().equals(refType) || oneToOne
                ref.getReferenceFieldType().isAssignableFrom(refType) ){

            for(Map.Entry<String, String> ent : ref.getReferenceColumns().entrySet()){
                Object obj = mapInfo.getObjectFieldValue(object, ent.getKey());
                refMapInfo.setObjectFieldValue(newObj, ent.getValue(), obj);
            }

            boolean haveSaved = false;
            if(refs!=null && refs.size()>0) {
                for (Object refObject : refs) {
                    if (refObjComparator.compare(refObject, newObj) == 0) {
                        //找到相同的对象 更新
                        innerUpdateObject(newObj);
                        haveSaved = true;
                    } else {
                        innerDeleteObject(refObject);
                    }
                }
            }
            if(!haveSaved){
                //没有相同的条目 新建
                innerSaveNewObject(newObj);
            }
            return 1;
        }else {
            //oneToMany 一对多的情况
            List<Object> newListObj = Set.class.isAssignableFrom(ref.getReferenceFieldType())?
                    new ArrayList<>((Set<?>) newObj):(List<Object>) newObj;

            for (Map.Entry<String, String> ent : ref.getReferenceColumns().entrySet()) {
                //ref.setObjectGetFieldValueFunc();
                Object obj = mapInfo.getObjectFieldValue(object, ent.getKey());
                for (Object subObj : newListObj) {
                    refMapInfo.setObjectFieldValue(subObj, ent.getValue(), obj);
                }
            }

            Triple<List<Object>, List<Pair<Object,Object>>, List<Object>>
                    comRes=
                    CollectionsOpt.compareTwoList((List<Object>)refs, newListObj,
                            refObjComparator );

            int resN = 0;
            if(comRes.getLeft() != null) {
                for (Object obj : comRes.getLeft()) {
                    innerSaveNewObject(obj);
                    resN ++;
                }
            }
            if(comRes.getRight() != null) {
                for (Object obj : comRes.getRight()) {
                    innerDeleteObject(obj);
                    resN ++;
                }
            }
            if(comRes.getMiddle() != null) {
                for (Pair<Object, Object> pobj : comRes.getMiddle()) {
                    if(GeneralJsonObjectDao.checkNeedUpdate(
                        CollectionsOpt.objectToMap(pobj.getLeft()),
                        CollectionsOpt.objectToMap(pobj.getRight()))) {
                        resN += innerUpdateObject(pobj.getRight());
                    }
                }
            }
            return resN;
        }
    }

    public int saveObjectReferences(T o) {
        int nRes = 0;
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo( getPoClass());
        if(mapInfo.hasReferences()) {
            for (SimpleTableReference ref : mapInfo.getReferences()) {
                nRes += saveObjectReference(o, ref.getReferenceName());
            }
        }
        return nRes;
    }

    /**
     * 这个方法会导致死循环，所以默认 只获取3层，并且这个方法没有重复测试 不推荐使用
     * 嵌套获取对象的引用，但是没有考虑 EntityWithDeleteTag EntityWithVersionTag 注解
     * @param id 主键
     * @return po 对象
     */
    @Deprecated
    public T getObjectCascadeById(Object id) {
        return jdbcTemplate.execute(
            (ConnectionCallback<T>) conn ->
                OrmDaoUtils.getObjectCascadeById(conn, id, (Class<T>) getPoClass(), DEFAULT_CASCADE_DEPTH));
    }

    /**
     * 这个方法会导致死循环，所以默认 只获取3层，并且这个方法没有重复测试 不推荐使用
     * 嵌套获取对象的引用，但是没有考虑 EntityWithDeleteTag EntityWithVersionTag 注解
     * @param object 对象
     * @return po 对象
     */
    @Deprecated
    public T fetchObjectReferencesCascade(T object) {
        return jdbcTemplate.execute(
            (ConnectionCallback<T>) conn ->
                OrmDaoUtils.fetchObjectReferencesCascade(conn, object, getPoClass(), DEFAULT_CASCADE_DEPTH));
    }
    /**
     * 这个方法会导致死循环，所以默认 只获取3层，并且这个方法没有重复测试 不推荐使用
     * 嵌套获取对象的引用，但是没有考虑 EntityWithDeleteTag EntityWithVersionTag 注解
     * @param object 对象
     * @return 变更数量
     */
    @Deprecated
    public Integer updateObjectCascade(T object) {
        return jdbcTemplate.execute(
            (ConnectionCallback<Integer>) conn ->
                OrmDaoUtils.updateObjectCascade(conn, object, DEFAULT_CASCADE_DEPTH));
    }
    /**
     * 这个方法会导致死循环，所以默认 只获取3层，并且这个方法没有重复测试 不推荐使用
     * 嵌套获取对象的引用，但是没有考虑 EntityWithDeleteTag EntityWithVersionTag 注解
     * @param object 对象
     * @return 变更数量
     */
    @Deprecated
    public Integer saveNewObjectCascade(T object) {
        return jdbcTemplate.execute(
            (ConnectionCallback<Integer>) conn ->
                OrmDaoUtils.saveNewObjectCascade(conn, object, DEFAULT_CASCADE_DEPTH));
    }

    public int deleteObjectsForceByProperties(Map<String, Object> properties,
            Collection<String> extentFilters, QueryUtils.IFilterTranslater powerTranslater){
        QueryAndNamedParams filterAndParams = buildFilterByParams(properties, extentFilters, powerTranslater);
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        String deleteSql = "delete from " + mapInfo.getTableName() + " where 1=1 " + filterAndParams.getQuery();
        return jdbcTemplate.execute(
            (ConnectionCallback<Integer>) conn ->
                DatabaseAccess.doExecuteNamedSql(conn, deleteSql, filterAndParams.getParams()));
    }
    public int deleteObjectsForceByProperties(Map<String, Object> properties){
        return deleteObjectsForceByProperties(properties, null, null);
    }

    public int deleteObjectsByProperties(Map<String, Object> properties,
                                          Collection<String> extentFilters, QueryUtils.IFilterTranslater powerTranslater) {
        boolean hasDeleteTag = EntityWithDeleteTag.class.isAssignableFrom(getPoClass());
        List<T> deleteList = listObjectsByProperties(properties, extentFilters, powerTranslater);
        int deleteSum = 0;
        if (deleteList != null) {
            for (T obj : deleteList) {
                if (hasDeleteTag) {
                    ((EntityWithDeleteTag) obj).setDeleted(true);
                    deleteSum += this.innerUpdateObject(obj);
                } else {
                    deleteSum += this.innerDeleteObjectForce(obj);
                }
            }
        }
        return deleteSum;
    }

    public int deleteObjectsByProperties(Map<String, Object> properties) {
        return deleteObjectsByProperties(properties, null, null);
    }

    /*==============================下面的代码是查询数据================================================*/
    /**
     * 查询所有数据
     * @return 返回所有数据 listAllObjects
     */
    public List<T> listObjects() {
        return jdbcTemplate.execute(
            (ConnectionCallback<List<T>>) conn ->
                OrmDaoUtils.listAllObjects(conn, (Class<T>) getPoClass()));
    }

    public T getObjectByProperties(Map<String, Object> properties) {
        return jdbcTemplate.execute(
            (ConnectionCallback<T>) conn ->
                OrmDaoUtils.getObjectByProperties(conn, properties, (Class<T>) getPoClass()));
    }

    public T getObjectByProperties(Map<String, Object> properties ,
                                   Collection<String> filters, QueryUtils.IFilterTranslater powerTranslater) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Pair<String, TableField[]> q = GeneralJsonObjectDao.buildFieldSqlWithFields(mapInfo, null, false);
        QueryAndNamedParams queryAndParams = buildFilterByParams(properties, filters, powerTranslater);

        String querySql = encapsulateFilterToSql(q.getLeft(), queryAndParams.getQuery(), null, null, false);
        queryAndParams.setQuery(querySql);

        try {
            return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                    OrmDaoUtils.queryNamedParamsSql(conn, queryAndParams,
                        (rs) -> OrmUtils.fetchObjectFormResultSet(rs, (Class<T>) getPoClass(), q.getRight())));
        } catch (ObjectException exception){
            logger.error("执行sql语句 " + querySql + " 时抛出异常" + exception.getMessage());
            return null;
        }
    }

    /**
     * 根据 前端传入的参数 对数据库中的数据进行计数
     * @param properties 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @return 返回的对象列表
     */
    public int countObjectByProperties(Map<String, Object> properties) {
        return countObjectByProperties(properties,null, null);
    }

    /**
     * 根据 前端传入的参数 对数据库中的数据进行计数
     * @param properties 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param filters 数据权限顾虑语句
     * @param powerTranslater 权限过滤引擎
     * @return 返回的对象列表
     */
    public int countObjectByProperties(Map<String, Object> properties,
                                       Collection<String> filters, QueryUtils.IFilterTranslater powerTranslater) {
        QueryAndNamedParams qap = buildQueryByParams( properties, null, filters, powerTranslater);
        String countSql = QueryUtils.buildGetCountSQLByReplaceFields(qap.getQuery());
        return NumberBaseOpt.castObjectToInteger(
            DatabaseOptUtils.getScalarObjectQuery(this, countSql, qap.getParams()), 0);
    }

    public List<T> listObjectsByProperties(final Map<String, Object> properties,
                                           Collection<String> filters, QueryUtils.IFilterTranslater powerTranslater) {
        QueryAndNamedParams qap = buildQueryByParams( properties, null, filters, powerTranslater);

        return jdbcTemplate.execute(
            (ConnectionCallback<List<T>>) conn -> OrmDaoUtils
                        .queryObjectsByNamedParamsSql(conn, qap.getQuery(), qap.getParams(), (Class<T>) getPoClass()));
    }

    public List<T> listObjectsByProperties(final Map<String, Object> properties,  Collection<String> filters,
                                           QueryUtils.IFilterTranslater powerTranslater, int startPos, int maxSize) {
        QueryAndNamedParams qap = buildQueryByParams( properties, null, filters, powerTranslater);
        return jdbcTemplate.execute(
            (ConnectionCallback<List<T>>) conn -> OrmDaoUtils
                .queryObjectsByNamedParamsSql(conn, qap.getQuery(), qap.getParams(), (Class<T>) getPoClass(), startPos, maxSize));
    }

    public List<T> listObjectsByProperties(final Map<String, Object> properties,  Collection<String> filters,
                                           QueryUtils.IFilterTranslater powerTranslater, PageDesc pageDesc) {
        QueryAndNamedParams qap = buildQueryByParams( properties, null, filters, powerTranslater);

        return jdbcTemplate.execute(
            (ConnectionCallback<List<T>>) conn -> {
                if(pageDesc != null && pageDesc.getPageSize() > 0 && pageDesc.getPageNo() > 0) {
                    pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn,
                        QueryUtils.buildGetCountSQLByReplaceFields(qap.getQuery()), qap.getParams()));
                    return OrmDaoUtils
                        .queryObjectsByNamedParamsSql(conn, qap.getQuery(), qap.getParams(), (Class<T>) getPoClass(),
                            pageDesc.getRowStart(), pageDesc.getPageSize());
                } else {
                    List<T> objects = OrmDaoUtils
                        .queryObjectsByNamedParamsSql(conn, qap.getQuery(), qap.getParams(), (Class<T>) getPoClass());
                    if(pageDesc != null && objects != null){
                        pageDesc.setTotalRows(objects.size());
                    }
                    return objects;
                }
            });
    }

    public List<T> listObjectsByProperties(final Map<String, Object> properties) {
        return listObjectsByProperties(properties, null, null);
    }

    /**
     * @param properties 查询过滤参数
     * @param startPos 返回的其实行 对应 mysql 的limit中的offset
     * @param maxSize 最大返回行
     * @return 返回的结果类别
     */
    public List<T> listObjectsByProperties(final Map<String, Object> properties, int startPos, int maxSize) {
        return listObjectsByProperties(properties, null, null, startPos, maxSize);
    }

    /**
     * @param properties 查询过滤参数
     * @param pageDesc 分页结构信息，系统会自动查询符合条件的数据记录数量，并设置到totalRows属性中
     * @return 返回的结果类别
     */
    public List<T> listObjectsByProperties(final Map<String, Object> properties, PageDesc pageDesc) {
        return listObjectsByProperties(properties, null, null, pageDesc);
    }

    private JSONArray listObjectsBySqlAsJson(String querySql, Object [] params,
                                             TableField[] fields, PageDesc pageDesc) {
        return jdbcTemplate.execute(
            (ConnectionCallback<JSONArray>) conn -> {
                try {
                    if(pageDesc != null && pageDesc.getPageSize() > 0 && pageDesc.getPageNo() > 0) {
                        String pageQuerySql =
                            QueryUtils.buildLimitQuerySQL(querySql,
                                pageDesc.getRowStart(), pageDesc.getPageSize(), false, DBType.mapDBType(conn));

                        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(
                            DatabaseAccess.getScalarObjectQuery(
                                conn, QueryUtils.buildGetCountSQLByReplaceFields(querySql), params)));
                        return GeneralJsonObjectDao.findObjectsBySql(conn, pageQuerySql, params, fields);
                    } else {
                        JSONArray ja = GeneralJsonObjectDao.findObjectsBySql(conn, querySql, params, fields);
                        if(pageDesc != null && ja!=null) {
                            pageDesc.setTotalRows(ja.size());
                        }
                        return ja;
                    }
                } catch (SQLException | IOException e) {
                    throw new ObjectException(e);
                }
            });
    }

    private JSONArray listObjectsBySqlAsJson(String querySql, Object [] params,
                                             TableField[] fields, int startPos, int maxSize) {
        return jdbcTemplate.execute(
            (ConnectionCallback<JSONArray>) conn -> {
                try {
                    return GeneralJsonObjectDao.findObjectsBySql(conn, QueryUtils.buildLimitQuerySQL(querySql,
                        startPos, maxSize, false, DBType.mapDBType(conn)), params, fields);
                } catch (SQLException | IOException e) {
                    throw new ObjectException(e);
                }
            });
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param properties 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param filters 数据权限顾虑语句
     * @param powerTranslater 权限过滤引擎
     * @return 返回的对象列表
     */
    public JSONArray listObjectsByPropertiesAsJson(final Map<String, Object> properties,
                                           Collection<String> filters, QueryUtils.IFilterTranslater powerTranslater) {
        return listObjectsByPropertiesAsJson(properties, filters, powerTranslater, null);
    }

    public JSONArray listObjectsByPropertiesAsJson(final Map<String, Object> properties) {
        return listObjectsByPropertiesAsJson(properties, null, null, null);
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param properties 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param filters 数据权限顾虑语句
     * @param powerTranslater 权限过滤引擎
     * @param startPos 分页信息
     * @param maxSize 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsByPropertiesAsJson(final Map<String, Object> properties,  Collection<String> filters,
                                           QueryUtils.IFilterTranslater powerTranslater, int startPos, int maxSize) {
        LeftRightPair<QueryAndNamedParams, TableField[]> queryAndFields = buildQueryByParamsWithFields( properties, null, filters, powerTranslater);
        QueryAndParams sqlQuery = QueryAndParams.createFromQueryAndNamedParams(queryAndFields.getLeft());
        return listObjectsBySqlAsJson(sqlQuery.getQuery(), sqlQuery.getParams(), queryAndFields.getRight(), startPos, maxSize);
    }
    public JSONArray listObjectsByPropertiesAsJson(final Map<String, Object> properties, int startPos, int maxSize) {
        return listObjectsByPropertiesAsJson(properties, null, null, startPos, maxSize);
    }
    /**
     * 根据 前端传入的参数 驱动查询
     * @param properties 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param filters 数据权限顾虑语句
     * @param powerTranslater 权限过滤引擎
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsByPropertiesAsJson(final Map<String, Object> properties,  Collection<String> filters,
                                           QueryUtils.IFilterTranslater powerTranslater, PageDesc pageDesc) {
        LeftRightPair<QueryAndNamedParams, TableField[]> queryAndFields = buildQueryByParamsWithFields( properties, null, filters, powerTranslater);
        QueryAndParams sqlQuery = QueryAndParams.createFromQueryAndNamedParams(queryAndFields.getLeft());

        return listObjectsBySqlAsJson(sqlQuery.getQuery(), sqlQuery.getParams(), queryAndFields.getRight(), pageDesc);

    }

    public JSONArray listObjectsByPropertiesAsJson(final Map<String, Object> properties, PageDesc pageDesc) {
        return listObjectsByPropertiesAsJson(properties, null, null, pageDesc);
    }


    public JSONArray listObjectsPartFieldByPropertiesAsJson(final Map<String, Object> properties, Collection<String> fields,
                                           Collection<String> filters, QueryUtils.IFilterTranslater powerTranslater) {
        return listObjectsPartFieldByPropertiesAsJson(properties, fields, filters, powerTranslater, null);
    }

    public JSONArray listObjectsPartFieldByPropertiesAsJson(final Map<String, Object> properties, Collection<String> fields,  Collection<String> filters,
                                           QueryUtils.IFilterTranslater powerTranslater, int startPos, int maxSize) {
        LeftRightPair<QueryAndNamedParams, TableField[]> queryAndFields = buildQueryByParamsWithFields( properties, fields, filters, powerTranslater);
        QueryAndParams sqlQuery = QueryAndParams.createFromQueryAndNamedParams(queryAndFields.getLeft());

        return listObjectsBySqlAsJson(sqlQuery.getQuery(), sqlQuery.getParams(), queryAndFields.getRight(), startPos, maxSize);
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param properties 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param fields 返回字段
     * @param filters 数据权限顾虑语句
     * @param powerTranslater 权限过滤引擎
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsPartFieldByPropertiesAsJson(final Map<String, Object> properties, Collection<String> fields,  Collection<String> filters,
                                           QueryUtils.IFilterTranslater powerTranslater, PageDesc pageDesc) {
        LeftRightPair<QueryAndNamedParams, TableField[]> queryAndFields = buildQueryByParamsWithFields( properties, fields, filters, powerTranslater);
        QueryAndParams sqlQuery = QueryAndParams.createFromQueryAndNamedParams(queryAndFields.getLeft());

        return listObjectsBySqlAsJson(sqlQuery.getQuery(), sqlQuery.getParams(), queryAndFields.getRight(), pageDesc);

    }

    public JSONArray listObjectsPartFieldByPropertiesAsJson(final Map<String, Object> properties, Collection<String> fields) {
        return listObjectsPartFieldByPropertiesAsJson(properties, fields, null, null, null);
    }

    public JSONArray listObjectsPartFieldByPropertiesAsJson(final Map<String, Object> properties, Collection<String> fields, int startPos, int maxSize) {
        return listObjectsPartFieldByPropertiesAsJson(properties, fields, null, null, startPos, maxSize);
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param properties 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param fields 返回字段
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsPartFieldByPropertiesAsJson(final Map<String, Object> properties, Collection<String> fields, PageDesc pageDesc) {
        return listObjectsPartFieldByPropertiesAsJson(properties, fields, null, null, pageDesc);

    }

    /*==============================下面的代码通过sql语句来查询数据的================================================*/
    public List<T> listObjectsBySql(String querySql, Map<String, Object> namedParams) {
        return jdbcTemplate.execute(
            (ConnectionCallback<List<T>>) conn ->
                OrmDaoUtils.queryObjectsByNamedParamsSql(
                    conn, querySql, namedParams, (Class<T>) getPoClass()));

    }

    public List<T> listObjectsBySql(String querySql, Object[] params) {
        return jdbcTemplate.execute(
            (ConnectionCallback<List<T>>) conn ->
                OrmDaoUtils.queryObjectsByParamsSql(conn, querySql, params, (Class<T>) getPoClass()));
    }

    /**
     * 由于性能问题，不推荐使用这个方法，分页查询一般都是用于前端展示的，建议使用  listObjectsByFilterAsJson
     *
     * @param whereSql 查询po 所以只有套写 where 以后部分
     * @param params   查询参数
     * @param tableAlias 表的别名
     * @return 返回对象
     */
    @Deprecated
    public List<T> listObjectsByFilter(String whereSql, Object[] params, String tableAlias) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        String fieldsSql = GeneralJsonObjectDao.buildFieldSql(mapInfo, tableAlias, 1);
        String querySql =  "select " + fieldsSql + " from " + mapInfo.getTableName()
            + ( StringUtils.isNotBlank(tableAlias)? " " + tableAlias + " " :" ") + whereSql;

        return listObjectsBySql(querySql, params);
    }

    /**
     * 由于性能问题，不推荐使用这个方法，分页查询一般都是用于前端展示的，建议使用  listObjectsByFilterAsJson
     *
     * @param whereSql    查询po 所以只有套写 where 以后部分
     * @param namedParams 查询参数
     * @param tableAlias 表的别名
     * @return 返回对象
     */
    public List<T> listObjectsByFilter(String whereSql, Map<String, Object> namedParams, String tableAlias) {
        QueryAndParams qap = QueryAndParams.createFromQueryAndNamedParams(whereSql, namedParams);
        return listObjectsByFilter(qap.getQuery(), qap.getParams(), tableAlias);
    }

    /**
     * 根据条件查询对象
     *
     * @param whereSql 只有 where 部分， 不能有from部分 这个式hibernate的区别
     * @param params   参数
     * @return 符合条件的对象
     */
    public List<T> listObjectsByFilter(String whereSql, Object[] params) {
        return listObjectsByFilter(whereSql, params, null);
    }

    /**
     * 根据条件查询对象
     *
     * @param whereSql    只有 where 部分， 不能有from部分 这个式hibernate的区别
     * @param namedParams 命名参数
     * @return 符合条件的对象
     */
    public List<T> listObjectsByFilter(String whereSql, Map<String, Object> namedParams) {
        return listObjectsByFilter(whereSql, namedParams, null);
    }

    private Pair<String, TableField[]> buildQuerySqlWithFieldsAndWhere(String whereSql, String tableAlias){
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Pair<String, TableField[]>  fieldsDesc =
            GeneralJsonObjectDao.buildFieldSqlWithFields(mapInfo, tableAlias, true);

         String querySql =  "select " + fieldsDesc.getLeft() + " from " + mapInfo.getTableName()
            + (StringUtils.isNotBlank(tableAlias)? " " + tableAlias + " " :" ") + whereSql;

        return new ImmutablePair<>(querySql,fieldsDesc.getRight());

    }
    /**
     *
     * @param whereSql 查询po 所以只有套写 where 以后部分
     * @param namedParams 查询参数
     * @param tableAlias 数据库别名
     * @param pageDesc 分页信息
     * @return 返回JSONArray
     */
    public JSONArray listObjectsByFilterAsJson(String whereSql, Map<String, Object> namedParams, String tableAlias, PageDesc pageDesc){
        Pair<String, TableField[]> fieldsDesc = buildQuerySqlWithFieldsAndWhere(whereSql, tableAlias);
        QueryAndParams sqlQuery = QueryAndParams.createFromQueryAndNamedParams(
            new QueryAndNamedParams(fieldsDesc.getLeft(), namedParams));
        return listObjectsBySqlAsJson(sqlQuery.getQuery(), sqlQuery.getParams(),
            fieldsDesc.getRight(), pageDesc);
    }

    public JSONArray listObjectsByFilterAsJson(String whereSql, Map<String, Object> namedParams,  PageDesc pageDesc) {
        return listObjectsByFilterAsJson(whereSql, namedParams, null, pageDesc);
    }

    /**
     *
     * @param whereSql 查询po 所以只有套写 where 以后部分
     * @param params 查询参数
     * @param tableAlias 数据库别名
     * @param pageDesc 分页信息
     * @return 返回JSONArray
     */
    public JSONArray listObjectsByFilterAsJson(String whereSql, Object[] params, String tableAlias, PageDesc pageDesc){
        Pair<String, TableField[]> fieldsDesc = buildQuerySqlWithFieldsAndWhere(whereSql, tableAlias);
        return listObjectsBySqlAsJson(fieldsDesc.getLeft(), params, fieldsDesc.getRight(), pageDesc);
    }

    public JSONArray listObjectsByFilterAsJson(String whereSql,Object[] params,  PageDesc pageDesc) {
        return listObjectsByFilterAsJson(whereSql, params, null, pageDesc);
    }
}
