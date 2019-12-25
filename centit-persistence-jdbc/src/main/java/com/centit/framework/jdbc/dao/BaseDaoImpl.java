package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.framework.core.po.EntityWithDeleteTag;
import com.centit.framework.core.po.EntityWithVersionTag;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
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
import com.centit.support.file.FileType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
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

    protected Map<String, String> filterField = null;
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
    public Connection getConnection() throws CannotGetJdbcConnectionException {
        return DataSourceUtils.getConnection(getDataSource());
    }

    /**
     * Close the given JDBC Connection, created via this DAO's DataSource,
     * if it isn't bound to the thread.
     *
     * @param con Connection to close
     * @see org.springframework.jdbc.datasource.DataSourceUtils#releaseConnection
     */
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

    public String encapsulateFilterToSql(String fieldsSql, String filterQuery, String tableAlias, String orderBySql) {
        boolean addAlias = StringUtils.isNotBlank(tableAlias);
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        StringBuilder sqlBuilder = new StringBuilder("select ");
        sqlBuilder.append(fieldsSql).append(" from ")
            .append(mapInfo.getTableName());
        if(addAlias){
            sqlBuilder.append(" ").append(tableAlias);
        }
        sqlBuilder.append(" where 1=1 {").append(mapInfo.getTableName());
        if(addAlias){
            sqlBuilder.append(" ").append(tableAlias);
        }
        sqlBuilder.append(" }").append(filterQuery);
        if(StringUtils.isNotBlank(orderBySql)){
            sqlBuilder.append(" order by " ).append(orderBySql);
        }
        return sqlBuilder.toString();
    }

    public String encapsulateFilterToFields(Collection<String> fields, String filterQuery, String tableAlias) {
        //QueryUtils.hasOrderBy(filterQuery)
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        String fieldsSql =
                ((fields != null && fields.size()>0)
                    ? GeneralJsonObjectDao.buildPartFieldSql(mapInfo, fields, tableAlias, true)
                    : GeneralJsonObjectDao.buildFieldSql(mapInfo, tableAlias, 1));
        return encapsulateFilterToSql(fieldsSql, filterQuery, tableAlias, mapInfo.getOrderBy());
    }

    /**
     * 可以在外部注入查询语句来屏蔽内部标量getFilterField中定义的查询语句
     * @return 外部语句
     */
    public String getExtendFilterQuerySql() {
        return ExtendedQueryPool.getExtendedSql(
                FileType.getFileExtName(getPoClass().getName()) + "_QUERY_0");
    }

    /**
     * 分析参数，这个主要用于解释，分析参数前面括号中的预处理标识
     * @param sParameter 传入的参数
     * @return ImmutablePair &lt; String, String &gt;
     */
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

    /**
     * 每个dao都需要重载这个函数已获得自定义的查询条件，否则listObjects、pageQuery就等价与listObjectsByProperties
     * 根据 getFilterField 中的内容初始化
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

                if (StringUtils.equalsAnyIgnoreCase(
                    skey, CodeBook.SELF_ORDER_BY, CodeBook.TABLE_SORT_FIELD,
                        CodeBook.TABLE_SORT_ORDER, CodeBook.ORDER_BY_HQL_ID)) {
                    continue;
                }
                if (skey.startsWith(CodeBook.NO_PARAM_FIX)) {
                    sBuilder.append(" [").append(skey).append("| and ")
                            .append(JpaMetadata.translateSqlPropertyToColumn(mapInfo, sSqlFormat, alias))
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
                            sBuilder.append(JpaMetadata.translateSqlPropertyToColumn(mapInfo, sSqlFormat, alias));
                        } else {
                            sBuilder.append(" [:");
                            if (StringUtils.isNotBlank(pretreatment)) {
                                sBuilder.append("(").append(pretreatment).append(")");
                            }
                            sBuilder.append(skey).append("| and ")
                                    .append(JpaMetadata.translateSqlPropertyToColumn(mapInfo, sSqlFormat, alias))
                                    .append(" ]");
                        }
                    }
                }// else
            }// for
        }//if(fieldFilter!=null)
        return sBuilder.toString();
    }

    private String daoEmbeddedFilter;
    /**
     * 每个dao都要初始化filterField这个对象，在 getFilterField 初始化，并且返回
     * @return 返回 getFilterField 属性
     */
    public abstract Map<String, String> getFilterField();
    /**
     * 是否为每一个没有配置 filterField 配置过滤条件的属性自动配置一个 equal 类型的过滤条件
     * @return 默认值为false
     */
    public boolean enableDefaultFilter(){
        return false;
    }
    public String buildDefaultFieldFilterSql() {
        if (daoEmbeddedFilter == null) {
            daoEmbeddedFilter = buildFieldFilterSql(null, enableDefaultFilter());
        }
        return daoEmbeddedFilter;
    }

    public String getFilterQuerySql() {
        String querySql = getExtendFilterQuerySql();
        if (StringUtils.isBlank(querySql)) {
            querySql = buildDefaultFieldFilterSql();
            return encapsulateFilterToFields(null, querySql, null);
        } else {
            if ("[".equals(Lexer.getFirstWord(querySql))) {
                return encapsulateFilterToFields(null, querySql, null);
            }
            return querySql;
        }
    }

    private void innerSaveNewObject(Object o) {
        if (o instanceof EntityWithVersionTag) {
            EntityWithVersionTag ewvto = (EntityWithVersionTag) o;
            TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(o.getClass());
            SimpleTableField field = mapInfo.findFieldByColumn(ewvto.obtainVersionProperty());
            Object obj = mapInfo.getObjectFieldValue(o, field);
            if(obj == null){
                mapInfo.setObjectFieldValue(o, field, ewvto.calcNextVersion());
            }
        }
        /* Integer execute = */
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveNewObject(conn, o));
    }

    public void saveNewObject(T o) {
        innerSaveNewObject(o);
    }

    private void deleteObjectWithVersion(final Object o){
        jdbcTemplate.execute(
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

    private void innerDeleteObjectForce(Object o) {
        if (o instanceof EntityWithVersionTag) {
            deleteObjectWithVersion(o);
        } else {
            /* Integer execute = */
            jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            OrmDaoUtils.deleteObject(conn, o));
        }
    }

    public void deleteObjectForce(T o) {
        innerDeleteObjectForce(o);
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

    private void innerDeleteObject(Object o) {
      /* Integer execute = */
        if (o instanceof EntityWithDeleteTag) {
            ((EntityWithDeleteTag) o).setDeleted(true);
            this.innerUpdateObject(o);
        } else {
            this.innerDeleteObjectForce(o);
        }
    }

    public void deleteObject(T o) {
        innerDeleteObject(o);
    }

    public void deleteObjectById(Object id) {
        /* Integer execute = */
        T o = getObjectById(id);
        if(o != null) {
            innerDeleteObject(o);
        }
    }

    public void deleteObjectsByProperties(Map<String, Object> filterMap) {
        boolean hasDeleteTag = EntityWithDeleteTag.class.isAssignableFrom(getPoClass());
        List<T> deleteList = listObjectsByProperties(filterMap);
        if (deleteList != null) {
            for (T obj : deleteList) {
                if (hasDeleteTag) {
                    ((EntityWithDeleteTag) obj).setDeleted(true);
                    this.innerUpdateObject(obj);
                } else {
                    this.innerDeleteObjectForce(obj);
                }
            }
        }
    }

    private int updateObjectWithVersion(final Object o, Collection<String> fields){
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn -> {
                    TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(o.getClass());
                    JsonObjectDao sqlDialect = GeneralJsonObjectDao.createJsonObjectDao(conn, mapInfo);
                    try {
                        OrmUtils.prepareObjectForUpdate(o, mapInfo, sqlDialect);
                    } catch (IOException e) {
                        throw new PersistenceException(e);
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
     * @throws PersistenceException 运行时异常
     * @return 是否 更新了数据库
     */
    public int updateObject(Collection<String> fields, T object)
            throws PersistenceException {
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
     * @throws PersistenceException 运行时异常
     * @return 是否 更新了数据库
     */
    public int updateObject(String[] fields, T object)
            throws PersistenceException {
        if (object instanceof EntityWithVersionTag) {
            return updateObjectWithVersion(object, CollectionsOpt.arrayToList(fields));
        }
        return  updateObject(CollectionsOpt.arrayToList(fields), object);
    }

    public int updateObjectWithNullField(T object, boolean includeLazy)
        throws PersistenceException {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        List<String> fields = includeLazy ? mapInfo.getAllFieldsName()
                  : mapInfo.getFieldsNameWithoutLazy();
        return updateObject(fields, object);
    }

    public int updateObjectWithNullField(T object)
        throws PersistenceException {
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
        if(ref==null || ref.getReferenceColumns().size()<1)
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
                }else {
                    ref.setObjectFieldValue(object, refs.get(0));
                }
            }else if(Set.class.isAssignableFrom(ref.getReferenceFieldType())){
                Set<Object> validRefDate = new HashSet<>(refs.size()+1);
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    for(Object refObject : refs)
                        if( ! ((EntityWithDeleteTag)refObject).isDeleted()){
                            validRefDate.add(refObject);
                        }
                }else {
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
                }else {
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

    public T getObjectByProperty(final String propertyName,final Object propertyValue) {
        return getObjectByProperties(CollectionsOpt.createHashMap(propertyName, propertyValue));
    }

    public T getObjectByProperties(Map<String, Object> properties) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectByProperties(conn, properties, (Class<T>) getPoClass()));
    }

    public List<T> listObjectsByProperty(final String propertyName,final Object propertyValue) {
        return listObjectsByProperties(CollectionsOpt.createHashMap(propertyName, propertyValue));
    }

    public List<T> listObjectsByProperties(final Map<String, Object> propertiesMap) {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn, propertiesMap, (Class<T>) getPoClass()));
    }

    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> filterMap, PageDesc pageDesc) {
        return jdbcTemplate.execute(
            (ConnectionCallback<JSONArray>) conn -> {
                TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
                GeneralJsonObjectDao sqlDialect = GeneralJsonObjectDao.createJsonObjectDao(conn, mapInfo);
                try {
                    if (pageDesc != null && pageDesc.getPageSize() > 0 && pageDesc.getPageNo() > 0) {
                        pageDesc.setTotalRows(sqlDialect.fetchObjectsCount(filterMap).intValue());
                        return sqlDialect.listObjectsByProperties(filterMap, pageDesc.getPageNo(), pageDesc.getPageSize());
                    } else {
                        JSONArray ja = sqlDialect.listObjectsByProperties(filterMap);
                        if(ja!=null) {
                            pageDesc.setTotalRows(ja.size());
                        }
                        return ja;
                    }
                } catch (IOException e) {
                    throw new DataAccessResourceFailureException(JSON.toJSONString(filterMap), new SQLException(e));
                }
            }
        );
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc) {
        if (pageDesc != null && pageDesc.getPageSize() > 0 && pageDesc.getPageNo() > 0) {
            return jdbcTemplate.execute(
                    (ConnectionCallback<List<T>>) conn -> {
                        pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn, filterMap, getPoClass()));
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

    public int countObjectByProperties(Map<String, Object> filterMap) {
        return jdbcTemplate.execute(
            (ConnectionCallback<Integer>) conn ->
                OrmDaoUtils.countObjectByProperties(conn, filterMap, getPoClass())
        );
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
            String selfOrderBy = fetchSelfOrderSql(filterMap);
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

    public List<T> listObjects(Map<String, Object> filterMap, PageDesc pageDesc) {
        String querySql = getFilterQuerySql();
        if (StringUtils.isBlank(querySql)) {
            return listObjectsByProperties(filterMap, pageDesc);
        } else {
            String selfOrderBy = fetchSelfOrderSql(filterMap);
            if (StringUtils.isNotBlank(selfOrderBy)) {
                querySql = QueryUtils.removeOrderBy(querySql) + " order by " + selfOrderBy;
            }
            QueryAndNamedParams qap = QueryUtils.translateQuery(querySql, filterMap);
            return jdbcTemplate.execute(
                  (ConnectionCallback<List<T>>) conn -> {
                    pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn,
                        QueryUtils.buildGetCountSQLByReplaceFields(qap.getSql()),qap.getParams()));
                    return OrmDaoUtils
                    .queryObjectsByNamedParamsSql(conn, qap.getQuery(), qap.getParams(), (Class<T>) getPoClass(),
                        pageDesc.getRowStart(), pageDesc.getPageSize());});
        }

    }

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

    private String buildQuerySqlByFilter(String whereSql,TableMapInfo mapInfo,String tableAlias){
        String fieldsSql = GeneralJsonObjectDao.buildFieldSql(mapInfo, tableAlias, 1);
        return "select " + fieldsSql + " from " + mapInfo.getTableName()
            + ( StringUtils.isNotBlank(tableAlias)? " " + tableAlias + " " :" ") + whereSql;
    }

    private String buildQuerySqlByFilter(String whereSql,String tableAlias){
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        return buildQuerySqlByFilter(whereSql,mapInfo,tableAlias);
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
        String querySql = buildQuerySqlByFilter(whereSql, tableAlias);
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

    public String fetchSelfOrderSql(Map<String, Object> filterMap) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        return GeneralJsonObjectDao.fetchSelfOrderSql(mapInfo,filterMap);
    }

    /**
     * 根据 前端传入的参数 对数据库中的数据进行计数
     * @param filterMap 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @return 返回的对象列表
     */
    public int countObject(Map<String, Object> filterMap) {
        return countObject(filterMap,null);
    }

    /**
     * 根据 前端传入的参数 对数据库中的数据进行计数
     * @param filterMap 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param filters 数据权限顾虑语句
     * @return 返回的对象列表
     */
    public int countObject(Map<String, Object> filterMap, Collection<String> filters) {
        String querySql = getFilterQuerySql();
        QueryAndNamedParams qap = QueryUtils.translateQuery(querySql, filters, filterMap, false);
        String countSql = QueryUtils.buildGetCountSQLByReplaceFields(qap.getQuery());
        return NumberBaseOpt.castObjectToInteger(
            DatabaseOptUtils.getScalarObjectQuery(this, countSql, qap.getParams()), 0);
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
                    throw new PersistenceException(e);
                }
            });
    }

    private JSONArray listObjectsByNamedSqlAsJson(String querySql, Map<String, Object> paramsMap,
                                            TableField[] fields, PageDesc pageDesc) {
        QueryAndParams sqlQuery = QueryAndParams.createFromQueryAndNamedParams(
            new QueryAndNamedParams(querySql, paramsMap));
        return listObjectsBySqlAsJson(sqlQuery.getQuery(), sqlQuery.getParams(),
              fields, pageDesc);
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param filterMap 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param fields 返回字段
     * @param filters 数据权限顾虑语句
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsPartFieldAsJson(Map<String, Object> filterMap, Collection<String> fields,
                                       Collection<String> filters, PageDesc pageDesc) {

        String filterSql = buildDefaultFieldFilterSql();
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Pair<String, TableField[]> q = ((fields != null && fields.size()>0)
            ? GeneralJsonObjectDao.buildPartFieldSqlWithFields(mapInfo, fields, null, true)
            : GeneralJsonObjectDao.buildFieldSqlWithFields(mapInfo, null, true));

        String selfOrderBy = GeneralJsonObjectDao.fetchSelfOrderSql(mapInfo, filterMap);
        String querySql = encapsulateFilterToSql(q.getLeft(), filterSql, null, selfOrderBy);
        QueryAndNamedParams qap = QueryUtils.translateQuery(querySql, filters, filterMap, false);
        return listObjectsByNamedSqlAsJson(qap.getQuery(), qap.getParams(), q.getRight(), pageDesc);
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param filterMap 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param fields 返回字段
     * @param filters 数据权限顾虑语句
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsPartFieldAsJson(Map<String, Object> filterMap, String [] fields,
                                       Collection<String> filters, PageDesc pageDesc) {
        return listObjectsPartFieldAsJson(filterMap, CollectionsOpt.createList(fields), filters , pageDesc);
    }

    /**
     * 根据 前端传入的参数 驱动查询
     * @param filterMap 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param fields 返回字段
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsPartFieldAsJson(Map<String, Object> filterMap, String [] fields, PageDesc pageDesc) {
        return listObjectsPartFieldAsJson(filterMap, CollectionsOpt.createList(fields), null , pageDesc);
    }
    /**
     * 根据 前端传入的参数 驱动查询
     * @param filterMap 前端输入的过滤条件，包括用户的基本信息（这个小service注入，主要用于数据权限的过滤）
     * @param filters 数据权限顾虑语句
     * @param pageDesc 分页信息
     * @return 返回的对象列表
     */
    public JSONArray listObjectsAsJson(Map<String, Object> filterMap, Collection<String> filters, PageDesc pageDesc) {
        return listObjectsPartFieldAsJson(filterMap, (Collection<String>) null, filters, pageDesc);
    }

    public JSONArray listObjectsAsJson(Map<String, Object> filterMap, PageDesc pageDesc){
        return listObjectsPartFieldAsJson(filterMap, (Collection<String>) null, null, pageDesc);
    }

    private Pair<String, TableField[]> buildQuerySqlWithFieldsandWhere(String whereSql, String tableAlias){
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
        Pair<String, TableField[]> fieldsDesc = buildQuerySqlWithFieldsandWhere(whereSql, tableAlias);
        return listObjectsByNamedSqlAsJson(fieldsDesc.getLeft(), namedParams, fieldsDesc.getRight(), pageDesc);
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
        Pair<String, TableField[]> fieldsDesc = buildQuerySqlWithFieldsandWhere(whereSql, tableAlias);
        return listObjectsBySqlAsJson(fieldsDesc.getLeft(), params, fieldsDesc.getRight(), pageDesc);
    }

    public JSONArray listObjectsByFilterAsJson(String whereSql,Object[] params,  PageDesc pageDesc) {
        return listObjectsByFilterAsJson(whereSql, params, null, pageDesc);
    }
}
