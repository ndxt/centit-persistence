package com.centit.framework.jdbc.dao;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.metadata.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author codefan
 *
 */
public class JsonObjectWork implements JsonObjectDao {

    protected static Logger logger = LoggerFactory.getLogger(JsonObjectWork.class);

    private TableInfo tableInfo;
    private BaseDaoImpl<?, ?> baseDao;

    public JsonObjectWork(){

    }

    public JsonObjectWork(TableInfo tableInfo){
        this.tableInfo = tableInfo;
    }

    public JsonObjectWork(BaseDaoImpl<?, ?> baseDao, TableInfo tableInfo){
        this.tableInfo = tableInfo;
        this.baseDao = baseDao;
    }

    public void setBaseDao(BaseDaoImpl<?, ?> baseDao) {
        this.baseDao = baseDao;
    }

    public void setTableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }


    private JsonObjectDao currentDao = null;

    public JsonObjectDao getCurrentDao() throws SQLException {
        if(currentDao==null)
            currentDao = GeneralJsonObjectDao.createJsonObjectDao(
                    baseDao.getConnection(), tableInfo);
        return currentDao;
    }

    public <T> T executeRealWork(JsonDaoExecuteWork<T> realWork)
            throws SQLException,IOException {
        Connection conn = baseDao.getConnection();
        try{
            JsonObjectDao currentDao = GeneralJsonObjectDao.createJsonObjectDao(
                    conn, tableInfo);
            T relRet = realWork.execute(currentDao);
            return relRet;
        } catch (SQLException e){
            logger.error("error code :" + e.getSQLState() + e.getLocalizedMessage(),e);
            throw e;
        } finally {
            baseDao.releaseConnection(conn);
        }
    }

    @Override
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public JSONObject getObjectById(final Object keyValue) throws SQLException, IOException {
        return executeRealWork(dao -> dao.getObjectById(keyValue) );
        //return executeRealWork(dao -> dao.getObjectById(keyValue);
    }

    @Override
    public JSONObject getObjectByProperties(final Map<String, Object> properties) throws SQLException, IOException {
        return executeRealWork(dao -> dao.getObjectByProperties(properties));
    }

    @Override
    public JSONArray listObjectsByProperties(final Map<String, Object> properties) throws SQLException, IOException {
        return executeRealWork(dao -> dao.listObjectsByProperties(properties));
    }

    @Override
    public JSONArray listObjectsByProperties(final Map<String, Object> properties,
            final int startPos,final int maxSize)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.listObjectsByProperties(properties,startPos,maxSize));
    }

    @Override
    public Long fetchObjectsCount(final Map<String, Object> properties) throws SQLException, IOException {
        return executeRealWork(dao -> dao.fetchObjectsCount(properties));
    }

    @Override
    public int saveNewObject(final Map<String, Object> object) throws SQLException {
        try {
            return executeRealWork(dao -> dao.saveNewObject(object));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public Map<String, Object> saveNewObjectAndFetchGeneratedKeys(Map<String, Object> object) throws SQLException, IOException {
         return executeRealWork(dao -> dao.saveNewObjectAndFetchGeneratedKeys(object));
    }

    /**
     * 更改部分属性
     *
     * @param fields 更改部分属性 属性名 集合，应为有的Map 不允许 值为null，这样这些属性 用map就无法修改为 null
     * @param object Map object
     */
    @Override
    public int updateObject(Collection<String> fields, Map<String, Object> object) throws SQLException {
        try {
            return executeRealWork(dao -> dao.updateObject(fields,object));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int updateObject(final Map<String, Object> object) throws SQLException {
        try {
            return executeRealWork(dao -> dao.updateObject(object));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    /**
     * 更改部分属性
     *
     * @param fields 更改部分属性 属性名 集合，应为有的Map 不允许 值为null，这样这些属性 用map就无法修改为 null
     * @param object Map object
     */
    @Override
    public int mergeObject(Collection<String> fields, Map<String, Object> object)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.mergeObject(fields,object));
    }

    @Override
    public int mergeObject(final Map<String, Object> object) throws SQLException, IOException {
        return executeRealWork(dao -> dao.mergeObject(object));
    }

    @Override
    public int updateObjectsByProperties(final Map<String, Object> fieldValues,
                                         final Map<String, Object> properties)
            throws SQLException {
        try {
            return executeRealWork(dao -> dao.updateObjectsByProperties(fieldValues,properties));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    /**
     * 根据条件批量更新 对象
     *
     * @param fields      更改部分属性 属性名 集合，应为有的Map 不允许 值为null，这样这些属性 用map就无法修改为 null
     * @param fieldValues Map fieldValues
     * @param properties Map properties
     * @throws SQLException SQLException
     * @return 批量更新数量
     */
    @Override
    public int updateObjectsByProperties(Collection<String> fields,
                                         Map<String, Object> fieldValues, Map<String, Object> properties)
            throws SQLException {
        try {
            return executeRealWork(dao -> dao.updateObjectsByProperties(fields,fieldValues,properties));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int deleteObjectById(final Object keyValue) throws SQLException {
        try {
            return executeRealWork(dao -> dao.deleteObjectById(keyValue));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int deleteObjectsByProperties(final Map<String, Object> properties) throws SQLException {
        try {
            return executeRealWork(dao -> dao.deleteObjectsByProperties(properties));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int insertObjectsAsTabulation(final List<Map<String,Object>> objects) throws SQLException {
        try {
            return executeRealWork(dao -> dao.insertObjectsAsTabulation(objects));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int deleteObjects(final List<Object> objects) throws SQLException {
        try {
            return executeRealWork(dao -> dao.deleteObjects(objects));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int deleteObjectsAsTabulation(final String propertyName, final Object propertyValue) throws SQLException {
        try {
            return executeRealWork(dao -> dao.deleteObjectsAsTabulation( propertyName,propertyValue));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int deleteObjectsAsTabulation(final Map<String, Object> properties) throws SQLException {
        try {
            return executeRealWork(dao -> dao.deleteObjectsAsTabulation(properties));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int replaceObjectsAsTabulation(final List<Map<String,Object>> newObjects, final List<Map<String,Object>> dbObjects) throws SQLException {
        try {
            return executeRealWork(dao -> dao.replaceObjectsAsTabulation(newObjects,dbObjects));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int replaceObjectsAsTabulation(final List<Map<String,Object>> newObjects, final String propertyName, final Object propertyValue)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.replaceObjectsAsTabulation(newObjects,propertyName,propertyValue));
    }

    @Override
    public int replaceObjectsAsTabulation(final List<Map<String,Object>> newObjects, final Map<String, Object> properties)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.replaceObjectsAsTabulation(newObjects,properties));
    }

    @Override
    public Long getSequenceNextValue(final String sequenceName) throws SQLException, IOException {
        return executeRealWork(dao -> dao.getSequenceNextValue(sequenceName));
    }

    @Override
    public List<Object[]> findObjectsBySql(final String sSql, final Object[] values) throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsBySql(sSql,values));
    }

    @Override
    public List<Object[]> findObjectsBySql(final String sSql, final Object[] values, final int pageNo, final int pageSize)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsBySql(sSql,values,pageNo,pageSize));
    }

    @Override
    public List<Object[]> findObjectsByNamedSql(final String sSql, final Map<String, Object> values)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsByNamedSql(sSql,values));
    }

    @Override
    public List<Object[]> findObjectsByNamedSql(final String sSql, final Map<String, Object> values,
            final int pageNo, final int pageSize)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsByNamedSql(sSql,values,pageNo,pageSize));
    }

    @Override
    public JSONArray findObjectsAsJSON(final String sSql, final Object[] values, final String[] fieldnames)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsAsJSON(sSql,values,fieldnames));
    }

    @Override
    public JSONArray findObjectsAsJSON(final String sSql, final Object[] values, final String[] fieldnames,
            final int pageNo, final int pageSize)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsAsJSON(sSql,values,fieldnames,pageNo,pageSize));
    }

    @Override
    public JSONArray findObjectsByNamedSqlAsJSON(final String sSql, final Map<String, Object> values,
            final String[] fieldnames)
            throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsByNamedSqlAsJSON(sSql,values,fieldnames));
    }

    @Override
    public JSONArray findObjectsByNamedSqlAsJSON(final String sSql, final Map<String, Object> values,
            final String[] fieldnames,final int pageNo, final int pageSize) throws SQLException, IOException {
        return executeRealWork(dao -> dao.findObjectsByNamedSqlAsJSON(sSql,values,fieldnames,pageNo,pageSize));
    }

    @Override
    public boolean doExecuteSql(final String sSql) throws SQLException {
        try {
            return executeRealWork(dao -> dao.doExecuteSql(sSql));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return false;
        }
    }

    @Override
    public int doExecuteSql(final String sSql,final Object[] values) throws SQLException {
        try {
            return executeRealWork(dao -> dao.doExecuteSql(sSql,values));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }

    @Override
    public int doExecuteNamedSql(final String sSql, final Map<String, Object> values) throws SQLException {
        try {
            return executeRealWork(dao -> dao.doExecuteNamedSql(sSql,values));
        } catch (IOException e) {
            logger.error("error code :" + e.getLocalizedMessage(),e);
            return 0;
        }
    }
}
