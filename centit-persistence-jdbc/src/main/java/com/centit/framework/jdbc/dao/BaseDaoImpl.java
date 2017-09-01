package com.centit.framework.jdbc.dao;

import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.framework.core.dao.PageDesc;
import com.centit.framework.core.dao.QueryParameterPrepare;
import com.centit.framework.jdbc.orm.JpaMetadata;
import com.centit.framework.jdbc.orm.OrmDaoUtils;
import com.centit.framework.jdbc.orm.TableMapInfo;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.utils.PersistenceException;
import com.centit.support.database.utils.QueryAndNamedParams;
import com.centit.support.database.utils.QueryUtils;
import com.centit.support.file.FileType;
import org.apache.commons.lang3.StringUtils;
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
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unused","unchecked"})
public abstract class BaseDaoImpl<T extends Serializable, PK extends Serializable>
{
    protected static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);
    private Class<?> poClass = null;
    private Class<?> pkClass = null;



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

    /**
     * 每个dao都需要重载这个函数已获得自定义的查询条件，否则listObjects、pageQuery就等价与listObjectsByProperties
     * @return FilterQuery
     */
    public String getFilterQuerySql(){
        String querySql = getExtendFilterQuerySql();
        if(StringUtils.isBlank( querySql))
            return null;
        if("[".equals(Lexer.getFirstWord(querySql))){
            return encapsulateFilterToSql(querySql);
        }
        return querySql;
    }

    public void deleteObject(T o){
       /* Integer execute = */
       jdbcTemplate.execute(
               (ConnectionCallback<Integer>) conn ->
                       OrmDaoUtils.deleteObject(conn, o));
    }

    public void deleteObjectById(PK id){
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.deleteObjectById(conn, id, getPoClass()));
    }

    public void saveNewObject(T o){
         /* Integer execute = */
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.saveNewObject(conn, o));
    }

    public T getObjectById(PK id){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectById(conn, id, (Class<T>)getPoClass()));
    }

    public T getObjectByProperties(Map<String, Object> properties){
        return jdbcTemplate.execute(
                (ConnectionCallback<T>) conn ->
                        OrmDaoUtils.getObjectByProperties(conn, properties, (Class<T>)getPoClass()));
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

    public void updateObject(T o){
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.updateObject(conn, o));
    }

    public void mergeObject(T o) {
        jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.mergeObject(conn, o));
    }

    public List<T> listObjects(){
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.listAllObjects(conn, (Class<T>)getPoClass()));
    }

    public int pageCount(String sql, Map<String, Object> filterMap){
        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
        return jdbcTemplate.execute(
                (ConnectionCallback<Integer>) conn ->
                        OrmDaoUtils.fetchObjectsCount(conn, qap.getSql(), qap.getParams() ));

    }

    public int pageCount(Map<String, Object> filterMap){
        String sql = getFilterQuerySql();
        if(StringUtils.isBlank( sql )){
            return jdbcTemplate.execute(
                    (ConnectionCallback<Integer>) conn ->
                            OrmDaoUtils.fetchObjectsCount(conn, filterMap, (Class<T>)getPoClass()));
        }
        return pageCount(sql, filterMap);
    }

    public List<T> pageQuery(String sql, Map<String, Object> filterMap, PageDesc pageDesc){

        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);

        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn -> {
                    pageDesc.setTotalRows(OrmDaoUtils.fetchObjectsCount(conn, QueryUtils.buildGetCountSQL(qap.getSql()),qap.getParams()));
                    return OrmDaoUtils.queryObjectsByNamedParamsSql(conn, sql, qap.getParams(), (Class<T>) getPoClass(),
                            pageDesc.getRowStart(), pageDesc.getPageSize() );
                }
        );
    }

    public List<T> pageQuery(Map<String, Object> filterMap, PageDesc pageDesc) {
        String sql = getFilterQuerySql();
        if(StringUtils.isBlank( sql )){
            return listObjectsByProperties( filterMap, pageDesc);
        }
        return pageQuery(sql, filterMap, pageDesc);
    }

    public List<T> pageQuery(String sql, Map<String, Object> filterMap){
        return pageQuery( sql, filterMap,
                QueryParameterPrepare.fetckPageDescParams(filterMap) ) ;
    }


    public List<T> pageQuery(Map<String, Object> filterMap) {
        return pageQuery(filterMap,
                QueryParameterPrepare.fetckPageDescParams(filterMap) ) ;
    }

    public List<T> listObjectsBySql(String sql, Map<String, Object> filterMap){

        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.queryObjectsByNamedParamsSql(conn,  qap.getSql() ,
                                qap.getParams(), (Class<T>) getPoClass()));
    }

    public List<T> listObjectsBySql(String sql, Object[] params){
        return jdbcTemplate.execute(
                (ConnectionCallback<List<T>>) conn ->
                        OrmDaoUtils.queryObjectsByParamsSql(conn,  sql, params , (Class<T>) getPoClass()));
    }

    public List<T> listObjects(Map<String, Object> filterMap) {
        String sql = getFilterQuerySql();
        if(StringUtils.isBlank( sql )){
            return listObjectsByProperties(filterMap);
        }
        return listObjectsBySql(sql, filterMap);
    }

    public List<T> listObjects(String propertyName, Object propertyValue) {
        return listObjects(QueryUtils.createSqlParamsMap( propertyName, propertyValue));
    }

}
