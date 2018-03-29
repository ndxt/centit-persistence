package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.framework.core.dao.QueryParameterPrepare;
import com.centit.framework.core.po.EntityWithDeleteTag;
import com.centit.support.algorithm.ListOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.ReflectionOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableReference;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmDaoUtils;
import com.centit.support.database.orm.TableMapInfo;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.database.utils.PersistenceException;
import com.centit.support.database.utils.QueryAndNamedParams;
import com.centit.support.database.utils.QueryUtils;
import com.centit.support.file.FileType;
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
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.*;

@SuppressWarnings({"unused","unchecked"})
public abstract class BaseDaoImpl<T extends Serializable, PK extends Serializable> {
    protected static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);
    private Class<?> poClass = null;
    private Class<?> pkClass = null;

    protected Map<String, String> filterField = null;
    protected JdbcTemplate jdbcTemplate;

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

    /**
     * 只更改对象的部分属性
     * @param fields 需要修改的部分属性
     * @param object 除了对应修改的属性 需要有相应的值，主键对应的属性也必须要值
     * @throws PersistenceException 运行时异常
     */
    public void updateObject(Collection<String> fields, T object)
            throws PersistenceException {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn, fields, object));
    }

    /**
     * 只更改对象的部分属性
     * @param fields 需要修改的部分属性
     * @param object 除了对应修改的属性 需要有相应的值，主键对应的属性也必须要值
     * @throws PersistenceException 运行时异常
     */
    public void updateObject(String[] fields, T object)
            throws PersistenceException {
        updateObject(ListOpt.arrayToList(fields), object);
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


    public T fetchObjectReference(T object, String columnName) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        SimpleTableReference ref = mapInfo.findReference(columnName);
        SimpleTableField field = mapInfo.findFieldByName(columnName);
        if(ref==null || field == null || ref.getReferenceColumns().size()<1)
            return object;

        Class<?> refType = ref.getTargetEntityType();
        TableMapInfo refMapInfo = JpaMetadata.fetchTableMapInfo( refType );
        if( refMapInfo == null )
            return object;

        Map<String, Object> properties = new HashMap<>(6);
        for(Map.Entry<String,String> ent : ref.getReferenceColumns().entrySet()){
            properties.put(ent.getValue(), ReflectionOpt.getFieldValue(object,ent.getKey()));
        }

        List<?> refs = jdbcTemplate.execute(
                (ConnectionCallback<List<?>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn, properties, refType));


        if(refs!=null && refs.size()>0) {
            if (//ref.getReferenceType().equals(refType) /*||
                    ref.getReferenceType().isAssignableFrom(refType) ){
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    for(Object refObject : refs)
                    if( ! ((EntityWithDeleteTag)refObject).isDeleted()){
                        field.setObjectFieldValue(object,refObject);
                        break;
                    }
                }else {
                    field.setObjectFieldValue(object, refs.get(0));
                }
            }else if(Set.class.isAssignableFrom(ref.getReferenceType())){
                Set<Object> validRefDate = new HashSet<>(refs.size()+1);
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    for(Object refObject : refs)
                        if( ! ((EntityWithDeleteTag)refObject).isDeleted()){
                            validRefDate.add(refObject);
                        }
                }else {
                    validRefDate.addAll(refs);
                }
                field.setObjectFieldValue(object,validRefDate);
            }else if(List.class.isAssignableFrom(ref.getReferenceType())){
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    List<Object>  validRefDate = new ArrayList<>(refs.size());
                    for(Object refObject : refs) {
                        if (!((EntityWithDeleteTag) refObject).isDeleted()) {
                            validRefDate.add(refObject);
                        }
                    }
                }else {
                    field.setObjectFieldValue(object, refs);
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

    public Integer saveObjectReference(T object, String columnName) {
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        SimpleTableReference ref = mapInfo.findReference(columnName);
        if(ref==null || ref.getReferenceColumns().size()<1)
            return 0;

        Object newObj = ref.getObjectFieldValue(object);
        Class<?> refType = ref.getTargetEntityType();
        TableMapInfo refMapInfo = JpaMetadata.fetchTableMapInfo( refType );
        if( refMapInfo == null )
            return 0;

        Map<String, Object> properties = new HashMap<>(6);
        for(Map.Entry<String,String> ent : ref.getReferenceColumns().entrySet()){
            properties.put(ent.getValue(), ReflectionOpt.getFieldValue(object,ent.getKey()));
        }
        List<?> refs = jdbcTemplate.execute(
                (ConnectionCallback<List<?>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn, properties, refType));
        if(newObj == null){ // delete all
            if(refs!=null && refs.size()>0){
                if( EntityWithDeleteTag.class.isAssignableFrom(refType)){
                    for(Object refObject : refs) {
                        if (!((EntityWithDeleteTag) refObject).isDeleted()) {
                            //设置删除标记
                            ((EntityWithDeleteTag) refObject).setDeleted(true);
                            jdbcTemplate.execute(
                                    (ConnectionCallback<Integer>) conn ->
                                            OrmDaoUtils.updateObject(conn, refObject));
                        }
                    }
                }else{ // 直接删除
                    return jdbcTemplate.execute(
                            (ConnectionCallback<Integer>) conn ->
                                    OrmDaoUtils.deleteObjectReference(conn, object,ref));
                }
            }
            return 1;
        }

        OrmDaoUtils.OrmObjectComparator refObjComparator = new OrmDaoUtils.OrmObjectComparator(refMapInfo);
        if (//ref.getReferenceType().equals(refType) || oneToOne
                ref.getReferenceType().isAssignableFrom(refType) ){
            for(Map.Entry<String, String> ent : ref.getReferenceColumns().entrySet()){
                Object obj = mapInfo.findFieldByName(ent.getKey()).getObjectFieldValue(object);
                refMapInfo.findFieldByName(ent.getValue()).setObjectFieldValue(newObj,obj);
            }
            boolean haveSaved = false;
            if(refs!=null && refs.size()>0) {
                for (Object refObject : refs) {
                    if (refObjComparator.compare(refObject, newObj) == 0) {
                        //找到相同的对象 更新
                        jdbcTemplate.execute((ConnectionCallback<Integer>)
                                conn -> OrmDaoUtils.updateObject(conn, newObj));
                        haveSaved = true;
                    } else {
                        if (EntityWithDeleteTag.class.isAssignableFrom(refType)) {
                            if (!((EntityWithDeleteTag) refObject).isDeleted()) {
                                //设置删除标记
                                ((EntityWithDeleteTag) refObject).setDeleted(true);
                                jdbcTemplate.execute((ConnectionCallback<Integer>) conn -> OrmDaoUtils.updateObject(conn, refObject));
                            }
                        } else {
                            jdbcTemplate.execute((ConnectionCallback<Integer>) conn -> OrmDaoUtils.deleteObject(conn, refObject));
                        }
                    }
                }
            }
            if(!haveSaved){
                //没有相同的条目 新建
                jdbcTemplate.execute((ConnectionCallback<Integer>)
                        conn -> OrmDaoUtils.saveNewObject(conn, newObj));
            }
            return 1;
        }else {
            //oneToMany 一对多的情况
            List<Object> newListObj = Set.class.isAssignableFrom(ref.getReferenceType())?
                    new ArrayList<>((Set<?>) newObj):(List<Object>) newObj;

            for(Map.Entry<String, String> ent : ref.getReferenceColumns().entrySet()){
                Object obj = mapInfo.findFieldByName(ent.getKey()).getObjectFieldValue(object);
                for(Object subObj : newListObj) {
                    refMapInfo.findFieldByName(ent.getValue()).setObjectFieldValue(subObj, obj);
                }
            }

            Triple<List<Object>, List<Pair<Object,Object>>, List<Object>>
                    comRes=
                    ListOpt.compareTwoList((List<Object>)refs, newListObj,
                            new OrmDaoUtils.OrmObjectComparator<>(mapInfo) );

            int resN = 0;
            if(comRes.getLeft() != null) {
                for (Object obj : comRes.getLeft()) {
                    resN += jdbcTemplate.execute((ConnectionCallback<Integer>)
                            conn -> OrmDaoUtils.saveNewObject(conn, obj));
                }
            }
            if(comRes.getRight() != null) {
                for (Object obj : comRes.getRight()) {
                    if( EntityWithDeleteTag.class.isAssignableFrom(refType)) {
                        if (!((EntityWithDeleteTag) obj).isDeleted()) {
                            //设置删除标记
                            ((EntityWithDeleteTag) obj).setDeleted(true);
                            resN += jdbcTemplate.execute((ConnectionCallback<Integer>)
                                    conn -> OrmDaoUtils.updateObject(conn, obj));
                        }
                    }else {
                        resN += jdbcTemplate.execute((ConnectionCallback<Integer>)
                                conn -> OrmDaoUtils.deleteObject(conn, obj));
                    }
                }
            }
            if(comRes.getMiddle() != null) {
                for (Pair<Object, Object> pobj : comRes.getMiddle()) {
                    resN += jdbcTemplate.execute((ConnectionCallback<Integer>)
                            conn -> OrmDaoUtils.updateObject(conn, pobj.getRight()));
                }
            }
            return resN;
        }
    }

    public Integer saveObjectReferences(T o) {
        int nRes = 0;
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo( getPoClass());
        if(mapInfo.hasReferences()) {
            for (SimpleTableReference ref : mapInfo.getReferences()) {
                nRes += saveObjectReference(o, ref.getReferenceName());
            }
        }
        return nRes;
    }


    public T getObjectByProperties(Map<String, Object> properties) {
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectByProperties(conn, properties, (Class<T>) getPoClass()));
    }

    public List<T> listObjectsByProperty(final String propertyName,final Object propertyValue) {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn,
                                QueryUtils.createSqlParamsMap(propertyName, propertyValue),
                                (Class<T>) getPoClass()));
    }

    public List<T> listObjectsByProperties(final Map<String, Object> propertiesMap) {
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn, propertiesMap, (Class<T>) getPoClass()));
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

    /**
     * querySql 用户检查order by 中的字段属性 对应的查询标识 比如，
     * select a+b as ab from table
     * 在 filterMap 中的 CodeBook.TABLE_SORT_FIELD (sort) 为 ab 字段 返回的排序语句为 a+b
     * @param querySql SQL语句 用来检查字段对应的查询语句 片段
     * @param filterMap 查询条件map其中包含排序属性
     * @return order by 字句
     */
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
