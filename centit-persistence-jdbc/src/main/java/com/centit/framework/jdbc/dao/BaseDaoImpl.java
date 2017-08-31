package com.centit.framework.jdbc.dao;

import com.centit.framework.core.dao.PageDesc;
import com.centit.framework.core.dao.QueryParameterPrepare;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmDaoSupport;
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

    public String getQueryFilterString(){
        String querySql = JpaMetadata.getExtendedSql(
                FileType.getFileExtName(getPoClass().getName())+"_QUERY_0");
        if(StringUtils.isBlank( querySql))
            return null;
        if("[".equals(Lexer.getFirstWord(querySql))){
            TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(getPoClass());
            if(mapInfo==null)
                return null;
            querySql = "select " + GeneralJsonObjectDao.buildFieldSql( mapInfo, null) +
                    " where 1=1 " + querySql;
        }
        return querySql;
    }

    public final Class<?> getPkClass() {
        if (pkClass == null) {
            fetchTypeParams();
        }
        return pkClass;
    }

    private DataSource dataSource;
    private OrmDaoSupport daoSupport;
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

    public void deleteObject(T o){
        daoSupport.deleteObject(o);
    }

    public void deleteObjectById(PK id){
        daoSupport.deleteObjectById(id, getPoClass());
    }

    public void saveNewObject(T o){
        daoSupport.saveNewObject(o);
    }

    public T getObjectById(PK id){
        return daoSupport.getObjectById(id,(Class<T>)getPoClass() );
    }

    public T getObjectByProperties(Map<String, Object> properties){
        return daoSupport.getObjectByProperties(properties,(Class<T>)getPoClass());
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap){
        return daoSupport.listObjectsByProperties( filterMap, (Class<T>)getPoClass());
    }

    public List<T> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc){
        pageDesc.setTotalRows(daoSupport.fetchObjectsCount( filterMap, (Class<T>)getPoClass() ) );
        return daoSupport.listObjectsByProperties( filterMap, (Class<T>)getPoClass(),
                pageDesc.getRowStart(), pageDesc.getPageSize());
    }

    public void updateObject(T o){
        daoSupport.updateObject(o);
    }

    public void mergeObject(T o) {
        daoSupport.mergeObject(o);
    }

    public List<T> listObjects(){
        return daoSupport.listAllObjects((Class<T>)getPoClass());
    }

    public int pageCount(String sql, Map<String, Object> filterMap){
        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
        return daoSupport.fetchObjectsCount(qap.getSql(), qap.getParams() );
    }

    public int pageCount(Map<String, Object> filterMap){
        String sql = getQueryFilterString();
        if(StringUtils.isBlank( sql )){
            return daoSupport.fetchObjectsCount( filterMap, (Class<T>)getPoClass() );
        }
        return pageCount(sql, filterMap);
    }

    public List<T> pageQuery(String sql, Map<String, Object> filterMap, PageDesc pageDesc){

        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
        pageDesc.setTotalRows(
                daoSupport.fetchObjectsCount( QueryUtils.buildGetCountSQL(qap.getSql()),qap.getParams()));
        return daoSupport.queryObjectsByNamedParamsSql( sql, qap.getParams(), (Class<T>) getPoClass(),
                pageDesc.getRowStart(), pageDesc.getPageSize() );
    }

    public List<T> pageQuery(Map<String, Object> filterMap, PageDesc pageDesc) {
        String sql = getQueryFilterString();
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

    public List<T> listObjects(String sql, Map<String, Object> filterMap){

        QueryAndNamedParams qap = QueryUtils.translateQuery( sql, filterMap);
        return daoSupport.queryObjectsByNamedParamsSql( sql, qap.getParams(), (Class<T>) getPoClass());
    }

    public List<T> listObjects(Map<String, Object> filterMap) {
        String sql = getQueryFilterString();
        if(StringUtils.isBlank( sql )){
            return listObjectsByProperties(filterMap);
        }
        return listObjects(sql, filterMap);
    }
}
