package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.framework.core.dao.PageDesc;
import com.centit.framework.core.dao.QueryParameterPrepare;
import com.centit.support.algorithm.ListOpt;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmDaoUtils;
import com.centit.support.database.orm.TableMapInfo;
import com.centit.support.database.utils.PersistenceException;
import com.centit.support.database.utils.QueryAndNamedParams;
import com.centit.support.database.utils.QueryUtils;
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
public abstract class BaseDaoImpl<T extends Serializable, PK extends Serializable>
{
    protected static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);
    private Class<?> poClass = null;
    private Class<?> pkClass = null;

    protected Map<String, String> filterField = null;
    protected JdbcTemplate jdbcTemplate;
    /**
     * Set the JDBC DataSource to obtain connections from.
     */
    @Resource
    public void setDataSource(DataSource dataSource) {
        if (this.jdbcTemplate == null || dataSource != this.jdbcTemplate.getDataSource()) {
            this.jdbcTemplate = new JdbcTemplate(dataSource);
        }
    }

    public final JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    public final DataSource getDataSource() {
        return (this.jdbcTemplate != null ? this.jdbcTemplate.getDataSource() : null);
    }
    /**
     * Get a JDBC Connection, either from the current transaction or a new one.
     * @return the JDBC Connection
     * @throws CannotGetJdbcConnectionException if the attempt to get a Connection failed
     * @see org.springframework.jdbc.datasource.DataSourceUtils#getConnection(javax.sql.DataSource)
     */
    protected final Connection getConnection() throws CannotGetJdbcConnectionException {
        return DataSourceUtils.getConnection(getDataSource());
    }

    /**
     * Close the given JDBC Connection, created via this DAO's DataSource,
     * if it isn't bound to the thread.
     * @param con Connection to close
     * @see org.springframework.jdbc.datasource.DataSourceUtils#releaseConnection
     */
    protected final void releaseConnection(Connection con) {
        DataSourceUtils.releaseConnection(con, getDataSource());
    }

    private final void fetchTypeParams(){
        ParameterizedType genType = (ParameterizedType) getClass()
                .getGenericSuperclass();
        Type[] params = genType.getActualTypeArguments();
        poClass = ((Class<?>) params[0]);
        pkClass = ((Class<?>) params[1]);
    }
    public final Class<?> getPoClass() {
        //return this.getClass().getTypeParameters()[0];
        if (poClass == null) {
            fetchTypeParams();
        }
        return poClass;
    }

    public final Class<?> getPkClass() {
        if (pkClass == null) {
            fetchTypeParams();
        }
        return pkClass;
    }

    public String encapsulateFilterToSql(String filterQuery){
        //QueryUtils.hasOrderBy(filterQuery)
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        if(mapInfo==null)
            throw new PersistenceException(PersistenceException.ORM_METADATA_EXCEPTION,
                    "没有对应的元数据信息："+getPoClass().getName());

        return  "select " + GeneralJsonObjectDao.buildFieldSql( mapInfo, null) +
                " where 1=1 " + filterQuery +
                ( StringUtils.isBlank( mapInfo.getOrderBy()) ? "" : " order by " +mapInfo.getOrderBy())
                ;
    }

    public String getExtendFilterQuerySql(){
        return ExtendedQueryPool.getExtendedSql(
                FileType.getFileExtName(getPoClass().getName())+"_QUERY_0");
    }

    public abstract Map<String, String> getFilterField();


    private static ImmutablePair<String,String> parseParameter(String sParameter){
        int e = sParameter.indexOf(')');
        if(e>0){
            int b = sParameter.indexOf('(') + 1;
            /* b =  b<0 ? 0 :  b+1;*/
            String paramPretreatment = sParameter.substring(b, e).trim();
            String paramAlias =  sParameter.substring(e+1).trim();
            return new ImmutablePair<>(paramAlias,paramPretreatment);
        }else
            return new ImmutablePair<>(sParameter,null);
    }

    public static Map<String,Pair<String,String>>
    getFilterFieldWithPretreatment( Map<String, String> fieldMap) {
        if(fieldMap==null)
            return null;
        Map<String,Pair<String,String>> filterFieldWithPretreatment =
                new HashMap<>(fieldMap.size()*2) ;

        if(fieldMap==null)
            return filterFieldWithPretreatment;

        for (Map.Entry<String, String> ent : fieldMap.entrySet()) {
            if(StringUtils.isNotBlank( ent.getKey() )) {
                ImmutablePair<String, String> paramMeta =
                        parseParameter(ent.getKey());
                filterFieldWithPretreatment.put(paramMeta.left,
                        new ImmutablePair<>(ent.getValue(), paramMeta.getRight()));
            }
        }
        return filterFieldWithPretreatment;
    }

    public static String translatePropertyNameToColumnName(TableMapInfo mapInfo, String sql , String alias){
        StringBuilder sqlb = new StringBuilder();
        Lexer lex = new Lexer(sql,Lexer.LANG_TYPE_SQL);
        boolean needTranslate = true;
        int prePos = 0; int preWordPos = 0;
        String aWord = lex.getAWord();
        boolean addAlias = StringUtils.isNotBlank(alias);
        while (aWord != null && !"".equals(aWord)) {
            if ("select".equalsIgnoreCase(aWord) || "from".equalsIgnoreCase(aWord)
                  /* || "group".equalsIgnoreCase(aWord) || "order".equalsIgnoreCase(aWord)*/ ){
                needTranslate = false;
            }else if ("where".equalsIgnoreCase(aWord)){
                needTranslate = true;
            }

            if(!needTranslate){
                preWordPos = lex.getCurrPos();
                aWord = lex.getAWord();
                continue;
            }

            if (":".equals(aWord)) {
                lex.getAWord(); // 跳过参数
                preWordPos = lex.getCurrPos();
                aWord = lex.getAWord();
            }

            if(Lexer.isLabel(aWord)){
                SimpleTableField col = mapInfo.findFieldByName(aWord);
                if(col!=null) {
                    if (preWordPos > prePos)
                        sqlb.append(sql.substring(prePos, preWordPos));
                    sqlb.append(addAlias?(" "+alias+"."):" ").append(col.getColumnName());
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
     * @return FilterQuery
     */
    public String buildFieldFilterSql(String alias, boolean useDefaultFilter){
        StringBuilder sBuilder= new StringBuilder();
        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        boolean addAlias = StringUtils.isNotBlank(alias);
        Map<String,Pair<String,String>> fieldFilter =
                getFilterFieldWithPretreatment(getFilterField());
        if(useDefaultFilter) {//添加默认的过滤条件
            mapInfo.getColumns().stream()
                    .filter(col -> fieldFilter == null || !fieldFilter.containsKey(col.getPropertyName()))
                    .forEach(col ->
                sBuilder.append(" [:").append(col.getPropertyName()).append("| and ")
                        .append(col.getColumnName()).append(" = :").append(col.getPropertyName())
                        .append(" ]"));
        }
        if(fieldFilter!=null){
            for (Map.Entry<String, Pair<String,String>> ent : fieldFilter.entrySet()) {
                String skey = ent.getKey();
                String sSqlFormat = ent.getValue().getLeft();

                if (skey.startsWith(CodeBook.NO_PARAM_FIX)) {
                    sBuilder.append(" [").append(skey).append("| and ")
                            .append( translatePropertyNameToColumnName(mapInfo, sSqlFormat ,alias) )
                            .append(" ]");
                }else{
                    String pretreatment = ent.getValue().getRight();
                    if (sSqlFormat.equalsIgnoreCase(CodeBook.EQUAL_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(skey);
                        if(col!=null) {
                            sBuilder.append(" [:");
                            if (StringUtils.isNotBlank(pretreatment)){
                                sBuilder.append("(").append(pretreatment).append(")");
                            }
                            sBuilder.append(skey).append("| and ")
                                    .append(addAlias?(alias+"."):"")
                                    .append(col.getColumnName()).append(" = :").append(col.getPropertyName())
                                    .append(" ]");
                        }
                    } else if (sSqlFormat.equalsIgnoreCase(CodeBook.LIKE_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(skey);
                        if(col!=null) {
                            sBuilder.append(" [:(")
                                    .append(StringUtils.isBlank(pretreatment)?"like":pretreatment)
                                    .append(")").append(skey).append("| and ")
                                    .append(addAlias?(alias+"."):"")
                                    .append(col.getColumnName()).append(" like :").append(col.getPropertyName())
                                    .append(" ]");
                        }
                    } else if (sSqlFormat.equalsIgnoreCase(CodeBook.IN_HQL_ID)) {
                        SimpleTableField col = mapInfo.findFieldByName(skey);
                        if(col!=null) {
                            sBuilder.append(" [:");
                            if (StringUtils.isNotBlank(pretreatment)){
                                sBuilder.append("(").append(pretreatment).append(")");
                            }
                            sBuilder.append(skey).append("| and ")
                                    .append(addAlias?(alias+"."):"")
                                    .append(col.getColumnName()).append(" in (:").append(col.getPropertyName())
                                    .append(") ]");
                        }
                    } else {
                        if( "[".equals(Lexer.getFirstWord(sSqlFormat))){
                            sBuilder.append( translatePropertyNameToColumnName(mapInfo, sSqlFormat ,alias) );
                        } else {
                            sBuilder.append(" [:");
                            if (StringUtils.isNotBlank(pretreatment)){
                                sBuilder.append("(").append(pretreatment).append(")");
                            }
                            sBuilder.append(skey).append("| and ")
                                    .append(translatePropertyNameToColumnName(mapInfo, sSqlFormat ,alias))
                                    .append(" ]");
                        }
                    }
                }// else
            }// for
        }//if(fieldFilter!=null)
        return sBuilder.toString();
    }

    private String daoEmbeddedFilter;
    public String buildDefaultFieldFilterSql(){
        if(daoEmbeddedFilter==null){
            daoEmbeddedFilter = buildFieldFilterSql(null,false);
        }
        return daoEmbeddedFilter;
    }

    public final String getFilterQuerySql(){
        String querySql = getExtendFilterQuerySql();
        if(StringUtils.isBlank( querySql)) {
            querySql = buildDefaultFieldFilterSql();
            return encapsulateFilterToSql(querySql);
        }else {
            if ("[".equals(Lexer.getFirstWord(querySql))) {
                return encapsulateFilterToSql(querySql);
            }
            return querySql;
        }
    }

    public void saveNewObject(T o){
         /* Integer execute = */
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveNewObject(conn, o));
    }

    public void deleteObject(T o){
       /* Integer execute = */
       jdbcTemplate.execute(
               (ConnectionCallback<Integer>) conn ->
                       OrmDaoUtils.deleteObject(conn, o));
    }

    public void deleteObjectById(Object id){
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.deleteObjectById(conn, id, getPoClass()));
    }

    public void deleteObjectsByProperties(Map<String, Object> filterMap){
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.deleteObjectByProperties(conn, filterMap, getPoClass()));
    }

    public void updateObject(T o){
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn, o));
    }

    public void updateObject(Collection<String> fields, T object)
            throws PersistenceException  {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn,fields, object));
    }

    public void batchUpdateObject(Collection<String> fields, T object, Map<String, Object> properties){
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.batchUpdateObject(conn,fields, object,properties));
    }


    public void updateObject(String [] fields , T object)
            throws PersistenceException  {
        updateObject(ListOpt.arrayToList(fields),object);
    }

    public void batchUpdateObject(String [] fields, T object, Map<String, Object> properties){
        batchUpdateObject(ListOpt.arrayToList(fields), object,properties);
    }


    public void mergeObject(T o) {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.mergeObject(conn, o));
    }

    public T getObjectById(Object id){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectById(conn, id, (Class<T>)getPoClass()));
    }

    public T getObjectIncludeLazyById(Object id){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectIncludeLazyById(conn, id, (Class<T>)getPoClass()));
    }


    public T fetchObjectLazyColumn(T o, String columnName){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectLazyColumn(conn, o, columnName ));
    }

    public T fetchObjectLazyColumns(T o){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectLazyColumns(conn, o));
    }


    public T fetchObjectReference(T o, String columnName){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectReference(conn, o, columnName ));
    }

    public T fetchObjectReferences(T o){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.fetchObjectReferences(conn, o));
    }

    public Integer saveObjectReference(T o, String columnName){
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveObjectReference(conn, o, columnName ));
    }

    public Integer saveObjectReferences(T o){
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveObjectReferences(conn, o ));
    }


    public T getObjectByProperties(Map<String, Object> properties){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectByProperties(conn, properties, (Class<T>)getPoClass()));
    }

    public List<T> listObjectsByProperty(String propertyName, Object value){
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn,
                                QueryUtils.createSqlParamsMap(propertyName, value),
                                (Class<T>)getPoClass()));
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap){
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listObjectsByProperties(conn, filterMap, (Class<T>)getPoClass()));
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc){
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn -> {
                    pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn, filterMap, (Class<T>)getPoClass() ) );
                    return OrmDaoUtils.listObjectsByProperties( conn, filterMap, (Class<T>)getPoClass(),
                            pageDesc.getRowStart(), pageDesc.getPageSize());
                }
        );

    }


    public List<T> listObjects(){
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listAllObjects(conn, (Class<T>)getPoClass()));
    }


    /**
     * 这个函数仅仅是为了兼容mybatis版本中的查询
     * @param filterMap
     * @return
     */
    public int pageCount(Map<String, Object> filterMap){
        String sql = getFilterQuerySql();
        if(StringUtils.isBlank( sql )){
            return jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            OrmDaoUtils.fetchObjectsCount(conn, filterMap, (Class<T>)getPoClass()));
        }else{
            QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
            return jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            OrmDaoUtils.fetchObjectsCount(conn, qap.getSql(), qap.getParams() ));
        }
    }

    /**
     * 这个函数仅仅是为了兼容mybatis版本中的查询
     * @param filterMap
     * @return
     */
    public List<T> pageQuery(Map<String, Object> filterMap) {
        String sql = getFilterQuerySql();
        PageDesc pageDesc = QueryParameterPrepare.fetckPageDescParams(filterMap);
        if(StringUtils.isBlank( sql )){
            return listObjectsByProperties( filterMap, pageDesc);
        }else{
            QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
            return jdbcTemplate.execute(
                    (ConnectionCallback<List<T>>) conn -> {
                        pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn,
                                /** 这个地方可以用replaceField 已提高效率 */
                                QueryUtils.buildGetCountSQLByReplaceFields(qap.getSql()),qap.getParams()));
                        return OrmDaoUtils.queryObjectsByNamedParamsSql(conn, sql, qap.getParams(), (Class<T>) getPoClass(),
                                pageDesc.getRowStart(), pageDesc.getPageSize() );
                    }
            );
        }
    }

    /* 下面所有的查询都返回 jsonArray */

    public JSONArray listObjectsBySqlAsJson(Map<String, Object> filterMap,  PageDesc pageDesc  ) {

        String querySql = getFilterQuerySql();

        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
        Pair<String,String[]> q = GeneralJsonObjectDao.buildFieldSqlWithFieldName(mapInfo,null);

        if(StringUtils.isBlank( querySql )){
            String filter = GeneralJsonObjectDao.buildFilterSql(mapInfo,null,filterMap.keySet());
            querySql = "select " + q.getLeft() +" from " +mapInfo.getTableName();
            if(StringUtils.isNotBlank(filter))
                querySql = querySql + " where " + filter;
            if(StringUtils.isNotBlank(mapInfo.getOrderBy()))
                querySql = querySql + " order by " + mapInfo.getOrderBy();
        }

        return DatabaseOptUtils.listObjectsBySqlAsJson(this, querySql, q.getRight() ,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql ), filterMap,   pageDesc  );
    }

    public JSONArray listObjectsBySqlAsJson(String querySql, Map<String, Object> filterMap,  PageDesc pageDesc  ) {
        return DatabaseOptUtils.listObjectsBySqlAsJson(this, querySql, null ,
                QueryUtils.buildGetCountSQLByReplaceFields( querySql ), filterMap,   pageDesc  );
    }

}
