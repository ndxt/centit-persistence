package com.centit.framework.jdbc.dao;

import com.centit.framework.core.dao.PageDesc;
import com.centit.framework.core.dao.QueryParameterPrepare;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmDaoSupport;
import com.centit.support.database.orm.PersistenceException;
import com.centit.support.database.orm.TableMapInfo;
import com.centit.support.database.utils.QueryAndNamedParams;
import com.centit.support.database.utils.QueryUtils;
import com.centit.support.file.FileType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public abstract class BaseDaoImpl<T extends Serializable, PK extends Serializable>
{
    protected static Logger logger = LoggerFactory.getLogger(BaseDaoImpl.class);
    private Class<?> poClass = null;
    private Class<?> pkClass = null;

    private DataSource dataSource;
    private OrmDaoSupport daoSupport;

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
        return JpaMetadata.getExtendedSql(
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

    /**
     * Set the JDBC DataSource to obtain connections from.
     */
    @Resource
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected final Connection getConnection() {
        return DataSourceUtils.getConnection( this.dataSource );
    }

    public final OrmDaoSupport getOrmDaoSupport()  {
        if(daoSupport==null && dataSource!=null){
            daoSupport = new OrmDaoSupport( getConnection() );
        }
        return daoSupport;
    }

    protected final JsonObjectDao getJsonObjectDao() {
        return getOrmDaoSupport().getJsonObjectDao(JpaMetadata.fetchTableMapInfo(getPoClass()) );
    }

    public Long getSequenceNextValue(final String sequenceName) {
        return getOrmDaoSupport().getSequenceNextValue(sequenceName);
    }

    public void deleteObject(T o){
        getOrmDaoSupport().deleteObject(o);
    }

    public void deleteObjectById(PK id){
        getOrmDaoSupport().deleteObjectById(id, getPoClass());
    }

    public void saveNewObject(T o){
        getOrmDaoSupport().saveNewObject(o);
    }

    public T getObjectById(PK id){
        return getOrmDaoSupport().getObjectById(id,(Class<T>)getPoClass() );
    }

    public T getObjectByProperties(Map<String, Object> properties){
        return getOrmDaoSupport().getObjectByProperties(properties,(Class<T>)getPoClass());
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap){
        return getOrmDaoSupport().listObjectsByProperties( filterMap, (Class<T>)getPoClass());
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc){
        pageDesc.setTotalRows(getOrmDaoSupport().fetchObjectsCount( filterMap, (Class<T>)getPoClass() ) );
        return getOrmDaoSupport().listObjectsByProperties( filterMap, (Class<T>)getPoClass(),
                pageDesc.getRowStart(), pageDesc.getPageSize());
    }

    public void updateObject(T o){
        getOrmDaoSupport().updateObject(o);
    }

    public void mergeObject(T o) {
        getOrmDaoSupport().mergeObject(o);
    }

    public List<T> listObjects(){
        return getOrmDaoSupport().listAllObjects((Class<T>)getPoClass());
    }

    public int pageCount(String sql, Map<String, Object> filterMap){
        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
        return getOrmDaoSupport().fetchObjectsCount(qap.getSql(), qap.getParams() );
    }

    public int pageCount(Map<String, Object> filterMap){
        String sql = getFilterQuerySql();
        if(StringUtils.isBlank( sql )){
            return getOrmDaoSupport().fetchObjectsCount( filterMap, (Class<T>)getPoClass() );
        }
        return pageCount(sql, filterMap);
    }

    public List<T> pageQuery(String sql, Map<String, Object> filterMap, PageDesc pageDesc){

        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
        pageDesc.setTotalRows(
                getOrmDaoSupport().fetchObjectsCount( QueryUtils.buildGetCountSQL(qap.getSql()),qap.getParams()));
        return getOrmDaoSupport().queryObjectsByNamedParamsSql( sql, qap.getParams(), (Class<T>) getPoClass(),
                pageDesc.getRowStart(), pageDesc.getPageSize() );
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
        return getOrmDaoSupport().queryObjectsByNamedParamsSql( qap.getSql()
                , qap.getParams(), (Class<T>) getPoClass());
    }

    public List<T> listObjectsBySql(String sql, Object[] params){
        return getOrmDaoSupport().queryObjectsByParamsSql( sql, params , (Class<T>) getPoClass());
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
