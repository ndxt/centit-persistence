package com.centit.framework.hibernate.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.support.database.jsonmaptable.*;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.DBType;
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

    private TableInfo tableInfo;
    private BaseDaoImpl<?, ?> baseDao;

    protected static Logger logger = LoggerFactory.getLogger(JsonObjectWork.class);

    public JsonObjectWork(){

    }

    public JsonObjectWork(TableInfo tableInfo){
        this.tableInfo = tableInfo;
    }

    public JsonObjectWork(BaseDaoImpl<?, ?> baseDao,TableInfo tableInfo){
        this.tableInfo = tableInfo;
        this.baseDao = baseDao;
    }

    public void setBaseDao(BaseDaoImpl<?, ?> baseDao) {
        this.baseDao = baseDao;
    }
    public void setTableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    private JsonObjectDao createDao(Connection connection){
        DBType dbtype = DBType.mapDBType(connection);
        switch (dbtype){
            case Oracle:
                return new OracleJsonObjectDao(connection ,tableInfo);
            case DB2:
                return new DB2JsonObjectDao(connection ,tableInfo);
            case SqlServer:
                return new SqlSvrJsonObjectDao(connection ,tableInfo);
            case MySql:
                return new MySqlJsonObjectDao(connection ,tableInfo);
            case H2:
                return new H2JsonObjectDao(connection ,tableInfo);
            case PostgreSql:
                return new PostgreSqlJsonObjectDao(connection ,tableInfo);
            case Access:
            default:
                throw new RuntimeException("不支持的数据库类型："+dbtype.toString());
        }
    }

    @Override
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public JSONObject getObjectById(final Object keyValue) throws SQLException, IOException {

        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.getObjectById(keyValue);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public JSONObject getObjectByProperties(final Map<String, Object> properties) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.getObjectByProperties(properties);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public JSONArray listObjectsByProperties(final Map<String, Object> properties) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.listObjectsByProperties(properties);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public JSONArray listObjectsByProperties(final Map<String, Object> properties,
            final int startPos,final int maxSize)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.listObjectsByProperties(properties,startPos,maxSize);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public Long fetchObjectsCount(final Map<String, Object> properties) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.fetchObjectsCount(properties);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public int saveNewObject(final Map<String, Object> object) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).saveNewObject(object) );
    }

    /**
     * 更改部分属性
     *
     * @param fields 更改部分属性 属性名 集合，应为有的Map 不允许 值为null，这样这些属性 用map就无法修改为 null
     * @param object Map object
     * @return  更改数量
     */
    @Override
    public int updateObject(final Collection<String> fields, final Map<String, Object> object) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).updateObject(fields, object) );
    }

    @Override
    public int updateObject(final Map<String, Object> object) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).updateObject(object) );
    }

    /**
     * 更改部分属性
     *
     * @param fields 更改部分属性 属性名 集合，应为有的Map 不允许 值为null，这样这些属性 用map就无法修改为 null
     * @param object Map object
     * @return  更改数量
     */
    @Override
    public int mergeObject(final Collection<String> fields, final Map<String, Object> object) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.mergeObject(fields,object);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return 0;
            }
        });
    }

    @Override
    public int mergeObject(final Map<String, Object> object) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.mergeObject(object);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return 0;
            }
        });
    }

    @Override
    public int updateObjectsByProperties(final Map<String, Object> fieldValues, final Map<String, Object> properties)
            throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            return dao.updateObjectsByProperties(fieldValues,properties);
        });
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
    public int updateObjectsByProperties(final Collection<String> fields,
                                         final Map<String, Object> fieldValues,
                                         final Map<String, Object> properties) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).updateObjectsByProperties(fields,fieldValues,properties) );
    }

    @Override
    public int deleteObjectById(final Object keyValue) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).deleteObjectById(keyValue) );
    }

    @Override
    public int deleteObjectsByProperties(final Map<String, Object> properties) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).deleteObjectsByProperties(properties) );
    }

    @Override
    public int insertObjectsAsTabulation(final List<Map<String, Object>> objects) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).insertObjectsAsTabulation(objects) );
    }

    @Override
    public int deleteObjects(final List<Object> objects) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).deleteObjects(objects) );
    }

    @Override
    public int deleteObjectsAsTabulation(final String propertyName, final Object propertyValue) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).deleteObjectsAsTabulation( propertyName,propertyValue) );
    }

    @Override
    public int deleteObjectsAsTabulation(final Map<String, Object> properties) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).deleteObjectsAsTabulation(properties) );
    }

    @Override
    public int replaceObjectsAsTabulation(final List<Map<String, Object>> newObjects, final List<Map<String, Object>> dbObjects) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            return dao.replaceObjectsAsTabulation(newObjects,dbObjects);
        });
    }

    @Override
    public int replaceObjectsAsTabulation(final List<Map<String, Object>> newObjects, final String propertyName, final Object propertyValue)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.replaceObjectsAsTabulation(newObjects,propertyName,propertyValue);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return 0;
            }
        });
    }

    @Override
    public int replaceObjectsAsTabulation(final List<Map<String, Object>> newObjects, final Map<String, Object> properties)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.replaceObjectsAsTabulation(newObjects,properties);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return 0;
            }
        });
    }

    @Override
    public Long getSequenceNextValue(final String sequenceName) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.getSequenceNextValue(sequenceName);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public List<Object[]> findObjectsBySql(final String sSql, final Object[] values) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsBySql(sSql,values);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public List<Object[]> findObjectsBySql(final String sSql, final Object[] values, final int pageNo, final int pageSize)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsBySql(sSql,values,pageNo,pageSize);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public List<Object[]> findObjectsByNamedSql(final String sSql, final Map<String, Object> values)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsByNamedSql(sSql,values);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public List<Object[]> findObjectsByNamedSql(final String sSql, final Map<String, Object> values,
            final int pageNo, final int pageSize)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsByNamedSql(sSql,values,pageNo,pageSize);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public JSONArray findObjectsAsJSON(final String sSql, final Object[] values, final String[] fieldnames)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsAsJSON(sSql,values,fieldnames);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public JSONArray findObjectsAsJSON(final String sSql, final Object[] values, final String[] fieldnames,
            final int pageNo, final int pageSize)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsAsJSON(sSql,values,fieldnames,pageNo,pageSize);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public JSONArray findObjectsByNamedSqlAsJSON(final String sSql, final Map<String, Object> values,
            final String[] fieldnames)
            throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsByNamedSqlAsJSON(sSql,values,fieldnames);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public JSONArray findObjectsByNamedSqlAsJSON(final String sSql, final Map<String, Object> values,
            final String[] fieldnames,final int pageNo, final int pageSize) throws SQLException, IOException {
        return baseDao.getCurrentSession().doReturningWork(connection -> {
            JsonObjectDao dao = createDao(connection);
            try {
                return dao.findObjectsByNamedSqlAsJSON(sSql,values,fieldnames,pageNo,pageSize);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                return null;
            }
        });
    }

    @Override
    public boolean doExecuteSql(final String sSql) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).doExecuteSql(sSql) );
    }

    @Override
    public int doExecuteSql(final String sSql,final Object[] values) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(connection ->
                createDao(connection).doExecuteSql(sSql,values));
    }

    @Override
    public int doExecuteNamedSql(final String sSql, final Map<String, Object> values) throws SQLException {
        return baseDao.getCurrentSession().doReturningWork(
                connection -> createDao(connection).doExecuteNamedSql(sSql,values));
    }
}
