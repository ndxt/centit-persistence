# centit-persistence-jdbc 模块

> Maven: `com.centit.framework:centit-persistence-jdbc`
> 包名: `com.centit.framework.jdbc.dao`, `com.centit.framework.core.dao`, `com.centit.framework.core.po`
> 核心依赖: centit-database + spring-jdbc + spring-tx + jakarta.persistence-api
> 模块描述: PDSqlOrm — 参数驱动的 ORM 平台，借用 Spring JDBC 管理数据库连接和事务

## 模块概述

南大先腾自研 ORM 框架的核心实现。不依赖 Hibernate/JPA 运行时，仅使用 JPA 注解（`jakarta.persistence`）作为元数据标记，通过 Spring `JdbcTemplate` 管理连接，底层委托给 centit-database 模块的 `OrmDaoUtils`、`DatabaseAccess` 等执行实际操作。

**核心设计**：所有实体 DAO 继承 `BaseDaoImpl<T, PK>` 即自动获得完整 CRUD、分页查询、级联引用、版本控制、逻辑删除等能力。仅需选择性重写 `getFilterField()` 定义自定义过滤条件。

---

## 核心类关系图

```
BaseDaoImpl<T, PK>  ←── 所有实体 DAO 继承此类
    │ @Autowired DataSource → JdbcTemplate
    │ 内部委托 → OrmDaoUtils / DatabaseAccess (centit-database)
    │
    ├── DatabaseOptUtils (静态工具门面，接收 BaseDaoImpl 参数)
    │       └── JdbcTemplateUtils (底层实现，接收 JdbcTemplate 参数)
    │
    ├── JsonObjectWork (实现 JsonObjectDao，通过 BaseDaoImpl 获取连接)
    │       └── JsonDaoExecuteWork<T> (回调函数式接口)
    │
    └── DDLOperationsWork (实现 DDLOperations，通过 BaseDaoImpl 获取连接)

辅助:
    DataFilter — 查询过滤器描述类（被 BaseDaoImpl 使用）
    CodeBook — 过滤器/排序常量
    EntityWithDeleteTag — 逻辑删除标记接口
    EntityWithVersionTag — 乐观锁版本控制标记接口
```

---

# 一、BaseDaoImpl<T extends Serializable, PK extends Serializable> (abstract class)

整个 PDSqlOrm 框架的核心基类，~1312 行。所有实体 DAO 都继承此类。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/BaseDaoImpl.java`
**泛型参数**: `T` — PO 实体类类型；`PK` — 主键类型（多字段联合主键可用 `Map<String, Object>`）

### 字段

| 字段 | 类型 | 可见性 | 说明 |
|------|------|--------|------|
| `logger` | `Logger` | protected static | 日志记录器 |
| `jdbcTemplate` | `JdbcTemplate` | protected | Spring JDBC 模板 |

### 数据源与连接管理

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `@Autowired setDataSource(DataSource dataSource)` | `void` | 自动注入数据源，创建 JdbcTemplate |
| `getJdbcTemplate()` | `JdbcTemplate` | 获取 Spring JDBC 模板 |
| `getDataSource()` | `DataSource` | 获取数据源 |
| `getConnection()` | `Connection` | 获取 JDBC 连接（不推荐直接使用） |
| `releaseConnection(Connection conn)` | `void` | 释放 JDBC 连接 |
| `getDBtype()` | `DBType` | 获取数据库类型 |

### 元数据与类型信息

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getPoClass()` | `Class<?>` | 获取 PO 实体类的 Class 对象 |
| `getPkClass()` | `Class<?>` | 获取主键类型的 Class 对象 |

### 过滤器与查询构建

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getFilterField()` | `Map<String, String>` | 获取过滤字段映射。**子类应重写此方法定义自定义过滤条件**。Map 的 key 为参数名，value 为 `"过滤SQL"` 格式 |
| `obtainInsideFilters(TableMapInfo mapInfo)` | `Map<String, DataFilter>` | 根据 getFilterField() 初始化内置过滤器 |
| `encapsulateFilterToSql(String fieldsSql, String filterQuery, String tableAlias, String orderBySql, boolean withExtFilter)` | `String` | 将过滤条件封装为完整 SQL |
| `encapsulateFilterToFields(Collection<String> fields, String filterQuery, String tableAlias, boolean withExtFilter)` | `String` | 封装查询字段和过滤条件为完整 SQL |
| `fetchSelfOrderSql(Map<String, Object> filterMap)` | `String` | 获取自定义排序 SQL（从 filterMap 中的排序参数提取） |
| `buildQueryByParamsWithFields(Map<String, Object> filterMap, Collection<String> fields, Collection<String> extentFilters, QueryUtils.IFilterTranslate powerTranslater)` | `LeftRightPair<QueryAndNamedParams, TableField[]>` | 构建带字段的参数化查询 |
| `buildQueryByParams(Map<String, Object> filterMap, Collection<String> fields, Collection<String> extentFilters, QueryUtils.IFilterTranslate powerTranslater)` | `QueryAndNamedParams` | 构建参数化查询（protected） |
| `buildFilterByParams(Map<String, Object> filterMap, Collection<String> extentFilters, QueryUtils.IFilterTranslate powerTranslater)` | `QueryAndNamedParams` | 构建过滤条件（protected） |

### 新增

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `saveNewObject(T object)` | `void` | 保存新对象。支持自增主键回写 |

### 删除

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `deleteObjectForce(T object)` | `int` | 强制物理删除（支持版本控制校验） |
| `deleteObjectForceById(Object id)` | `int` | 按 ID 强制物理删除 |
| `deleteObject(T object)` | `int` | 删除对象（若实体实现 `EntityWithDeleteTag` 则逻辑删除，否则物理删除） |
| `deleteObjectById(Object id)` | `int` | 按 ID 删除 |
| `deleteObjectsForceByProperties(Map<String, Object> properties, Collection<String> extentFilters, QueryUtils.IFilterTranslate powerTranslater)` | `int` | 按属性条件强制物理删除多条 |
| `deleteObjectsForceByProperties(Map<String, Object> properties)` | `int` | 简化版：按属性强制物理删除 |
| `deleteObjectsByProperties(Map<String, Object> properties, Collection<String> extentFilters, QueryUtils.IFilterTranslate powerTranslater)` | `int` | 按属性条件删除（支持逻辑删除） |
| `deleteObjectsByProperties(Map<String, Object> properties)` | `int` | 简化版：按属性删除 |

### 更新

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `updateObject(T object)` | `int` | 更新整个对象（支持版本控制校验） |
| `updateObject(Collection<String> fields, T object)` | `int` | 更新对象的指定字段 |
| `updateObject(String[] fields, T object)` | `int` | 更新对象的指定字段（数组版） |
| `updateObjectWithNullField(T object, boolean includeLazy)` | `int` | 更新对象（包括 null 字段） |
| `updateObjectWithNullField(T object)` | `int` | 更新对象（包括 null 字段，不含懒加载） |
| `mergeObject(T object)` | `int` | 合并对象（存在则更新，不存在则新增） |

### 存在性检查

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `checkObjectExists(T object)` | `int` | 检查对象是否存在 |
| `checkObjectExistsById(Object id)` | `int` | 按 ID 检查对象是否存在 |

### 单对象查询

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getObjectById(Object id)` | `T` | 按 ID 获取对象 |
| `getObjectExcludeLazyById(Object id)` | `T` | 按 ID 获取对象（不加载懒加载字段） |
| `getObjectWithReferences(Object id)` | `T` | 按 ID 获取对象并加载所有引用（子表） |
| `getObjectByProperties(Map<String, Object> properties)` | `T` | 按属性条件获取单个对象 |
| `getObjectByProperties(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater)` | `T` | 按属性条件获取单个对象（带权限过滤） |

### 懒加载

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `fetchObjectLazyColumn(T object, String columnName)` | `T` | 获取对象的指定懒加载字段 |
| `fetchObjectLazyColumns(T object)` | `T` | 获取对象的所有懒加载字段 |

### 引用（子表）操作

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `fetchObjectReference(T object, String columnName)` | `T` | 加载对象的指定引用（子表） |
| `fetchObjectReferences(T object)` | `T` | 加载对象的所有引用（子表） |
| `deleteObjectReference(T object, String columnName)` | `int` | 删除对象的指定引用（支持逻辑删除） |
| `deleteObjectReferences(T object)` | `int` | 删除对象的所有引用（支持逻辑删除） |
| `deleteObjectReferenceForce(T object, String columnName)` | `int` | 强制删除对象的指定引用 |
| `deleteObjectReferencesForce(T object)` | `int` | 强制删除对象的所有引用 |
| `saveObjectReference(T object, String columnName)` | `int` | 保存对象的指定引用（智能对比增删改） |
| `saveObjectReferences(T object)` | `int` | 保存对象的所有引用 |

### 级联操作（@Deprecated）

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getObjectCascadeById(Object id)` | `T` | 级联获取对象（默认 3 层深度） |
| `fetchObjectReferencesCascade(T object)` | `T` | 级联加载引用（默认 3 层深度） |
| `updateObjectCascade(T object)` | `Integer` | 级联更新（默认 3 层深度） |
| `saveNewObjectCascade(T object)` | `Integer` | 级联保存（默认 3 层深度） |

### 列表查询（返回 List\<T\>）

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `listObjects()` | `List<T>` | 查询所有数据 |
| `listObjectsByProperties(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater)` | `List<T>` | 按属性条件查询（带权限过滤） |
| `listObjectsByProperties(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater, int startPos, int maxSize)` | `List<T>` | 分页查询（startPos 0-based） |
| `listObjectsByProperties(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater, PageDesc pageDesc)` | `List<T>` | 分页查询（PageDesc） |
| `listObjectsByProperties(Map<String, Object> properties)` | `List<T>` | 简化版 |
| `listObjectsByProperties(Map<String, Object> properties, int startPos, int maxSize)` | `List<T>` | 简化版分页 |
| `listObjectsByProperties(Map<String, Object> properties, PageDesc pageDesc)` | `List<T>` | 简化版 PageDesc 分页 |

### 计数

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `countObjectByProperties(Map<String, Object> properties)` | `int` | 按属性条件计数 |
| `countObjectByProperties(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater)` | `int` | 按属性条件计数（带权限过滤） |

### 列表查询（返回 JSONArray）

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `listObjectsByPropertiesAsJson(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater)` | `JSONArray` | 属性条件查询返回 JSON |
| `listObjectsByPropertiesAsJson(Map<String, Object> properties)` | `JSONArray` | 简化版 |
| `listObjectsByPropertiesAsJson(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater, int startPos, int maxSize)` | `JSONArray` | 分页查询返回 JSON |
| `listObjectsByPropertiesAsJson(Map<String, Object> properties, int startPos, int maxSize)` | `JSONArray` | 简化版分页 |
| `listObjectsByPropertiesAsJson(Map<String, Object> properties, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater, PageDesc pageDesc)` | `JSONArray` | PageDesc 分页返回 JSON |
| `listObjectsByPropertiesAsJson(Map<String, Object> properties, PageDesc pageDesc)` | `JSONArray` | 简化版 PageDesc |
| `listObjectsPartFieldByPropertiesAsJson(Map<String, Object> properties, Collection<String> fields, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater)` | `JSONArray` | 部分字段查询返回 JSON |
| `listObjectsPartFieldByPropertiesAsJson(Map<String, Object> properties, Collection<String> fields, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater, int startPos, int maxSize)` | `JSONArray` | 部分字段分页查询 |
| `listObjectsPartFieldByPropertiesAsJson(Map<String, Object> properties, Collection<String> fields, Collection<String> filters, QueryUtils.IFilterTranslate powerTranslater, PageDesc pageDesc)` | `JSONArray` | 部分字段 PageDesc 分页 |
| `listObjectsPartFieldByPropertiesAsJson(Map<String, Object> properties, Collection<String> fields)` | `JSONArray` | 简化版部分字段查询 |
| `listObjectsPartFieldByPropertiesAsJson(Map<String, Object> properties, Collection<String> fields, int startPos, int maxSize)` | `JSONArray` | 简化版部分字段分页 |
| `listObjectsPartFieldByPropertiesAsJson(Map<String, Object> properties, Collection<String> fields, PageDesc pageDesc)` | `JSONArray` | 简化版部分字段 PageDesc |

### 原生 SQL 查询（返回 List\<T\>）

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `listObjectsBySql(String querySql, Map<String, Object> namedParams)` | `List<T>` | 命名参数 SQL 查询 |
| `listObjectsBySql(String querySql, Object[] params)` | `List<T>` | 位置参数 SQL 查询 |
| `listObjectsByFilter(String whereSql, Object[] params, String tableAlias)` | `List<T>` | where 条件查询（位置参数） |
| `listObjectsByFilter(String whereSql, Map<String, Object> namedParams, String tableAlias)` | `List<T>` | where 条件查询（命名参数） |
| `listObjectsByFilter(String whereSql, Object[] params)` | `List<T>` | 简化版 where 条件查询 |
| `listObjectsByFilter(String whereSql, Map<String, Object> namedParams)` | `List<T>` | 简化版 where 条件查询（命名参数） |

### 原生 SQL 查询（返回 JSONArray）

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `listObjectsByFilterAsJson(String whereSql, Map<String, Object> namedParams, String tableAlias, PageDesc pageDesc)` | `JSONArray` | where 条件分页查询返回 JSON |
| `listObjectsByFilterAsJson(String whereSql, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 简化版 |
| `listObjectsByFilterAsJson(String whereSql, Object[] params, String tableAlias, PageDesc pageDesc)` | `JSONArray` | where 条件分页查询（位置参数） |
| `listObjectsByFilterAsJson(String whereSql, Object[] params, PageDesc pageDesc)` | `JSONArray` | 简化版 |

---

# 二、DatabaseOptUtils (abstract class, 工具类)

面向 `BaseDaoImpl` 的高级工具门面类，所有方法均为 static。内部委托给 `JdbcTemplateUtils`。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/DatabaseOptUtils.java`

### 请求参数收集

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static collectRequestParameters(HttpServletRequest request)` | `Map<String, Object>` | 从 HTTP 请求收集参数并预处理 |

### PO 元数据

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static extraPoAllFieldNames(Class<?> poClass)` | `List<String>` | 获取 PO 类的所有字段名 |
| `static extraPoAllFieldNamesAsArray(Class<?> poClass)` | `String[]` | 获取 PO 类的所有字段名（数组） |

### 存储过程

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static callFunction(BaseDaoImpl<?, ?> baseDao, String procName, int sqlType, Object... paramObjs)` | `Object` | 调用数据库函数 |
| `static callProcedure(BaseDaoImpl<?, ?> baseDao, String procName, Object... paramObjs)` | `boolean` | 调用存储过程 |

### SQL 执行

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static doExecuteSql(BaseDaoImpl<?, ?> baseDao, String sql)` | `boolean` | 执行 SQL（无参数） |
| `static doExecuteSql(BaseDaoImpl<?, ?> baseDao, String sql, Object[] values)` | `int` | 执行带参 SQL |
| `static doExecuteNamedSql(BaseDaoImpl<?, ?> baseDao, String sql, Map<String, Object> values)` | `int` | 执行命名参数 SQL |

### JSON 查询（命名参数）

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, String queryCountSql, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 完整参数版分页查询 |
| `static listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 自动 count 分页查询 |
| `static listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, Map<String, Object> namedParams)` | `JSONArray` | 无分页查询 |
| `static listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 无字段名分页查询 |
| `static listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> params)` | `JSONArray` | 最简版 |
| `static listObjectsByNamedSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 分页版 |

### JSON 查询（位置参数）

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, String queryCountSql, Object[] params, PageDesc pageDesc)` | `JSONArray` | 完整参数版分页查询 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, Object[] params)` | `JSONArray` | 位置参数查询 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, Object[] params, PageDesc pageDesc)` | `JSONArray` | 位置参数分页查询 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql, Object[] params, PageDesc pageDesc)` | `JSONArray` | 无字段名分页查询 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params, String[] fieldnames)` | `JSONArray` | 带字段名查询 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params)` | `JSONArray` | 最简版 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params, PageDesc pageDesc)` | `JSONArray` | 分页版 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql)` | `JSONArray` | 无参数版 |
| `static listObjectsBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, PageDesc pageDesc)` | `JSONArray` | 无参数分页版 |

### List\<Object[]\> 查询

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql)` | `List<Object[]>` | SQL 查询 |
| `static listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params)` | `List<Object[]>` | 带参数查询 |
| `static listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql, PageDesc pageDesc)` | `List<Object[]>` | 分页查询 |
| `static listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql, Object[] params, PageDesc pageDesc)` | `List<Object[]>` | 带参数分页查询 |
| `static listObjectsBySql(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params, PageDesc pageDesc)` | `List<Object[]>` | 简化版分页 |
| `static listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> namedParams)` | `List<Object[]>` | 命名参数查询 |
| `static listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql, Map<String, Object> namedParams, PageDesc pageDesc)` | `List<Object[]>` | 命名参数分页查询 |
| `static listObjectsByNamedSql(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> namedParams, PageDesc pageDesc)` | `List<Object[]>` | 简化版 |

### 参数驱动 SQL 查询

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, String queryCountSql, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 完整参数版 |
| `static listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 自动 count 版 |
| `static listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String[] fieldNames, Map<String, Object> namedParams)` | `JSONArray` | 无分页版 |
| `static listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, String queryCountSql, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 无字段名版 |
| `static listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> namedParams)` | `JSONArray` | 最简版 |
| `static listObjectsByParamsDriverSqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> namedParams, PageDesc pageDesc)` | `JSONArray` | 分页版 |

### 单对象查询

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params, String[] fieldName)` | `JSONObject` | 位置参数单对象查询 |
| `static getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Object[] params)` | `JSONObject` | 简化版 |
| `static getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> params, String[] fieldName)` | `JSONObject` | 命名参数版 |
| `static getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql, Map<String, Object> params)` | `JSONObject` | 简化命名参数版 |
| `static getObjectBySqlAsJson(BaseDaoImpl<?, ?> baseDao, String querySql)` | `JSONObject` | 无参数版 |

### 标量查询

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sql, Map<String, Object> values)` | `Object` | 命名参数标量查询 |
| `static getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sql, Object[] values)` | `Object` | 位置参数标量查询 |
| `static getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sql)` | `Object` | 无参数标量查询 |
| `static getScalarObjectQuery(BaseDaoImpl<?, ?> baseDao, String sql, Object value)` | `Object` | 单参数标量查询 |

### 序列与批量操作

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static getSequenceNextValue(BaseDaoImpl<?, ?> baseDao, String sequenceName)` | `Long` | 获取序列下一个值 |
| `static batchSaveNewObjects(BaseDaoImpl<?, ?> baseDao, Collection<?> objects)` | `int` | 批量保存 |
| `static batchUpdateObjects(BaseDaoImpl<?, ?> baseDao, Collection<?> objects)` | `int` | 批量更新 |
| `static batchMergeObjects(BaseDaoImpl<?, ?> baseDao, Collection<?> objects)` | `int` | 批量合并 |
| `static batchDeleteObjects(BaseDaoImpl<?, ?> baseDao, Collection<?> objects)` | `int` | 批量删除 |
| `static batchUpdateObject(BaseDaoImpl<?, ?> baseDao, Collection<String> fields, T object, Map<String, Object> properties)` | `Integer` | 批量修改指定字段 |
| `static batchUpdateObject(BaseDaoImpl<?, ?> baseDao, String[] fields, T object, Map<String, Object> properties)` | `Integer` | 批量修改（数组版） |
| `static batchUpdateObject(BaseDaoImpl<?, ?> baseDao, Class<?> type, Map<String, Object> propertiesValue, Map<String, Object> propertiesFilter)` | `Integer` | 批量修改（按类型） |
| `static replaceObjectsAsTabulation(BaseDaoImpl<?, ?> baseDao, List<T> oldDbObject, List<T> newObjects)` | `Integer` | 替换表格数据 |
| `static replaceObjectsAsTabulation(BaseDaoImpl<?, ?> baseDao, Class<?> type, List<Map<String, Object>> oldDbObject, List<Map<String, Object>> newObjects)` | `Integer` | 替换表格数据（按类型） |
| `static doGetDBType(BaseDaoImpl<?, ?> baseDao)` | `DBType` | 获取数据库类型 |

---

# 三、JdbcTemplateUtils (abstract class, 工具类)

底层 JDBC 操作工具类，直接操作 `JdbcTemplate`。是 `DatabaseOptUtils` 的底层实现，方法签名几乎一一对应。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/JdbcTemplateUtils.java`

所有方法均为 `static`，第一个参数为 `JdbcTemplate`。方法列表与 `DatabaseOptUtils` 完全对应（将 `BaseDaoImpl` 参数替换为 `JdbcTemplate`），此处不再重复列出。

**关键区别**：`DatabaseOptUtils` 面向 DAO 开发者（接收 BaseDaoImpl），`JdbcTemplateUtils` 面向需要直接操作 JdbcTemplate 的底层场景。

---

# 四、JsonObjectWork (class, implements JsonObjectDao)

`JsonObjectDao` 接口的 JDBC 实现。通过 `BaseDaoImpl` 获取连接，委托给 `GeneralJsonObjectDao`。用于无 PO 实体类的动态表操作。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/JsonObjectWork.java`

### 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `tableInfo` | `TableInfo` | 表结构元数据 |
| `baseDao` | `BaseDaoImpl<?, ?>` | 基础 DAO 引用 |
| `currentDao` | `JsonObjectDao` | 延迟初始化的实际执行 DAO |

### 构造方法

| 签名 | 说明 |
|------|------|
| `JsonObjectWork()` | 无参构造 |
| `JsonObjectWork(TableInfo tableInfo)` | 带表信息构造 |
| `JsonObjectWork(BaseDaoImpl<?, ?> baseDao, TableInfo tableInfo)` | 完整构造 |

### 关键方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `setBaseDao(BaseDaoImpl<?, ?> baseDao)` | `void` | 设置基础 DAO |
| `setTableInfo(TableInfo tableInfo)` | `void` | 设置表信息 |
| `getCurrentDao()` | `JsonObjectDao` | 获取当前 DAO 连接 |
| `executeRealWork(JsonDaoExecuteWork<T> realWork)` | `T` | 执行自定义 DAO 工作（模板方法） |

其余方法实现 `JsonObjectDao` 接口的所有方法（完整列表见 [centit-database-metadata-ddl.md](centit-database-metadata-ddl.md) 中 JsonObjectDao 部分）。

---

# 五、DDLOperationsWork (class, implements DDLOperations)

DDL 操作的 JDBC 适配器，通过 `BaseDaoImpl` 获取连接并委托给 `GeneralDDLOperations`。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/DDLOperationsWork.java`

### 构造方法

| 签名 | 说明 |
|------|------|
| `DDLOperationsWork()` | 无参构造 |
| `DDLOperationsWork(BaseDaoImpl<?, ?> baseDao)` | 带 DAO 构造 |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `setBaseDao(BaseDaoImpl<?, ?> baseDao)` | `void` | 设置基础 DAO |
| `getDDLOperations()` | `DDLOperations` | 获取 DDL 操作实例（延迟创建） |
| `createSequence(String sequenceName)` | `void` | 创建序列 |
| `createTable(TableInfo tableInfo)` | `void` | 创建表 |
| `dropTable(String tableCode)` | `void` | 删除表 |
| `addColumn(String tableCode, TableField column)` | `void` | 添加列 |
| `modifyColumn(String tableCode, TableField oldColumn, TableField column)` | `void` | 修改列 |
| `dropColumn(String tableCode, String columnCode)` | `void` | 删除列 |
| `renameColumn(String tableCode, String columnCode, TableField column)` | `void` | 重命名列 |
| `reconfigurationColumn(String tableCode, String columnCode, TableField column)` | `void` | 重新配置列 |
| `makeCreateViewSql(String selectSql, String viewName)` | `String` | 生成创建视图 SQL |
| `makeCreateSequenceSql(String sequenceName)` | `String` | 生成创建序列 SQL |
| `makeCreateTableSql(TableInfo tableInfo, boolean fieldStartNewLine)` | `String` | 生成建表 SQL |
| `makeTableColumnComments(TableInfo tableInfo, int commentContent)` | `List<String>` | 生成列注释 SQL |
| `makeDropTableSql(String tableCode)` | `String` | 生成删表 SQL |
| `makeAddColumnSql(String tableCode, TableField column)` | `String` | 生成添加列 SQL |
| `makeModifyColumnSql(String tableCode, TableField oldColumn, TableField column)` | `String` | 生成修改列 SQL |
| `makeDropColumnSql(String tableCode, String columnCode)` | `String` | 生成删除列 SQL |
| `makeRenameColumnSql(String tableCode, String columnCode, TableField column)` | `String` | 生成重命名列 SQL |
| `makeReconfigurationsColumnSqls(String tableCode, String columnCode, TableField column)` | `List<String>` | 生成重新配置列 SQL |

---

# 六、DataFilter (class)

查询过滤条件描述类，封装一个过滤条件的所有信息。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/DataFilter.java`

### 构造方法

| 签名 | 说明 |
|------|------|
| `DataFilter(String pretreatmentSql, String filterSql)` | 根据预处理 SQL 和过滤 SQL 创建过滤器 |

### 字段（含 getter）

| 字段 | 类型 | 说明 |
|------|------|------|
| `formula` | `String` | 过滤公式/参数名 |
| `pretreatment` | `String` | 参数预处理方式 |
| `valueName` | `String` | 值参数名（SQL 中的命名参数名） |
| `filterSql` | `String` | 过滤 SQL 片段 |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `setPretreatment(String pretreatment)` | `void` | 设置预处理方式 |
| `setFilterSql(String filterSql)` | `void` | 设置过滤 SQL |

---

# 七、JsonDaoExecuteWork\<T\> (函数式接口)

回调接口，用于 `JsonObjectWork.executeRealWork()` 中执行自定义数据库操作。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/JsonDaoExecuteWork.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `T execute(JsonObjectDao conn) throws SQLException, IOException` | 在 JsonObjectDao 连接上执行自定义操作 |

---

# 八、CodeBook (class, 常量)

查询过滤器和排序相关的常量定义。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/core/dao/CodeBook.java`

| 常量名 | 类型 | 值 | 说明 |
|--------|------|-----|------|
| `LIKE_HQL_ID` | `String` | `"LIKE"` | LIKE 匹配过滤标识 |
| `EQUAL_HQL_ID` | `String` | `"EQUAL"` | 等于匹配过滤标识 |
| `NO_PARAM_FIX` | `String` | `"NP_"` | 无参数前缀 |
| `IN_HQL_ID` | `String` | `"IN"` | IN 匹配过滤标识 |
| `SELF_ORDER_BY` | `String` | (引用 GeneralJsonObjectDao) | 用户自定义排序描述 |
| `SELF_ORDER_BY2` | `String` | (引用 GeneralJsonObjectDao) | 用户自定义排序描述 2 |
| `TABLE_SORT_FIELD` | `String` | (引用 GeneralJsonObjectDao) | 排序字段 |
| `TABLE_SORT_ORDER` | `String` | (引用 GeneralJsonObjectDao) | 排序顺序 |
| `MYBATIS_ORDER_FIELD` | `String` | `"mybatisOrderBy"` | MyBatis 专用排序字段 |

---

# 九、EntityWithDeleteTag (接口)

逻辑删除标记接口。PO 实体实现此接口后，`deleteObject`/`deleteObjectById` 不会物理删除，而是设置删除标记。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/core/po/EntityWithDeleteTag.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `isDeleted()` | `boolean` | 判断是否已删除 |
| `setDeleted(boolean isDeleted)` | `void` | 设置删除标志 |

---

# 十、EntityWithVersionTag (接口)

乐观锁版本控制标记接口。PO 实体实现此接口后，`updateObject`/`deleteObject` 会校验版本号，版本不一致则操作失败（返回 0）。

**路径**: `centit-persistence-jdbc/src/main/java/com/centit/framework/core/po/EntityWithVersionTag.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `calcNextVersion()` | `Object` | 计算下一个版本号 |
| `obtainVersionProperty()` | `String` | 返回版本字段的属性名（必须有 `@Column` 注解） |

---

# 使用示例

### 示例 1：定义实体 PO

```java
@Data
@Table(name = "T_OFFICE_WORKER")
public class OfficeWorker implements EntityWithDeleteTag, EntityWithVersionTag, Serializable {

    @Id
    @Column(name = "WORKER_ID")
    @ValueGenerator(strategy = GeneratorType.UUID)
    private String workerId;

    @Column(name = "WORKER_NAME")
    private String workerName;

    @Column(name = "WORKER_AGE")
    private Integer workerAge;

    @Column(name = "HEAD_IMAGE")
    @Basic(fetch = FetchType.LAZY)  // 懒加载
    private byte[] headImage;

    @Column(name = "IS_DELETE")
    private Boolean isDelete;

    @Column(name = "VERSION_NO")
    private Integer versionNo;

    @Column(name = "CREATE_DATE")
    @ValueGenerator(strategy = GeneratorType.FUNCTION, value = "sysdate", occasion = GeneratorTime.NEW)
    private Date createDate;

    @OneToMany
    @JoinColumn(name = "WORKER_ID", referencedColumnName = "WORKER_ID")
    private List<Career> workerCareers;  // 子表引用

    // EntityWithDeleteTag
    @Override public boolean isDeleted() { return Boolean.TRUE.equals(isDelete); }
    @Override public void setDeleted(boolean deleted) { this.isDelete = deleted; }

    // EntityWithVersionTag
    @Override public Object calcNextVersion() { return (versionNo == null ? 1 : versionNo + 1); }
    @Override public String obtainVersionProperty() { return "versionNo"; }
}
```

### 示例 2：定义 DAO

```java
@Repository
public class OfficeWorkerDao extends BaseDaoImpl<OfficeWorker, String> {

    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filters = new HashMap<>();
        // 格式: "参数名" -> "预处理方式|过滤SQL"
        filters.put("workerName", "LIKE:WORKER_NAME");      // LIKE 模糊匹配
        filters.put("workerAge", "NP:WORKER_AGE");           // 无预处理，精确匹配
        filters.put("beginDate", "DATE:CREATE_DATE>=:beginDate"); // 日期范围
        filters.put("endDate", "DATE:CREATE_DATE<=:endDate");
        return filters;
    }
}
```

### 示例 3：Service 层使用 DAO

```java
@Service
public class OfficeWorkerService {

    @Autowired
    private OfficeWorkerDao officeWorkerDao;

    // 新增
    public void create(OfficeWorker worker) {
        officeWorkerDao.saveNewObject(worker);
    }

    // 按属性查询 + 分页
    public List<OfficeWorker> list(Map<String, Object> filter, PageDesc pageDesc) {
        return officeWorkerDao.listObjectsByProperties(filter, pageDesc);
    }

    // 按 ID 获取（含子表）
    public OfficeWorker getDetail(String id) {
        OfficeWorker worker = officeWorkerDao.getObjectWithReferences(id);
        return worker;
    }

    // 更新（版本控制）
    public void update(OfficeWorker worker) {
        int affected = officeWorkerDao.updateObject(worker);
        if (affected == 0) {
            throw new RuntimeException("版本冲突，数据已被其他人修改");
        }
    }

    // 删除（逻辑删除）
    public void delete(String id) {
        officeWorkerDao.deleteObjectById(id);  // 自动 setDeleted(true)
    }

    // 强制物理删除
    public void forceDelete(String id) {
        officeWorkerDao.deleteObjectForceById(id);
    }

    // 保存子表
    public void saveCareers(String workerId, List<Career> careers) {
        OfficeWorker worker = officeWorkerDao.getObjectById(workerId);
        worker.setWorkerCareers(careers);
        officeWorkerDao.saveObjectReference(worker, "workerCareers");
    }
}
```

### 示例 4：使用 DatabaseOptUtils 执行自定义 SQL

```java
@Repository
public class ReportDao extends BaseDaoImpl<OfficeWorker, String> {

    // 命名参数分页查询返回 JSON
    public JSONArray queryReport(Map<String, Object> params, PageDesc pageDesc) {
        String sql = "SELECT w.WORKER_NAME, COUNT(c.CAREER_ID) as career_count " +
                     "FROM T_OFFICE_WORKER w LEFT JOIN T_CAREER c ON w.WORKER_ID = c.WORKER_ID " +
                     "WHERE w.IS_DELETE = 0 GROUP BY w.WORKER_NAME";
        return DatabaseOptUtils.listObjectsByNamedSqlAsJson(this, sql, params, pageDesc);
    }

    // 执行更新 SQL
    public int batchUpdateStatus(List<String> ids, String status) {
        String sql = "UPDATE T_OFFICE_WORKER SET STATUS = :status WHERE WORKER_ID IN (:ids)";
        Map<String, Object> params = new HashMap<>();
        params.put("status", status);
        params.put("ids", ids);
        return DatabaseOptUtils.doExecuteNamedSql(this, sql, params);
    }
}
```

### 示例 5：使用 JsonObjectWork 操作动态表

```java
@Service
public class DynamicTableService {

    @Autowired
    private OfficeWorkerDao baseDao;

    public void workWithDynamicTable(SimpleTableInfo tableInfo) {
        JsonObjectWork jsonWork = new JsonObjectWork(baseDao, tableInfo);

        // 插入
        Map<String, Object> data = new HashMap<>();
        data.put("ID", "001");
        data.put("NAME", "测试");
        jsonWork.saveNewObject(data);

        // 查询
        JSONObject obj = jsonWork.getObjectById("001");

        // 自定义操作
        jsonWork.executeRealWork(dao -> {
            return dao.findObjectsBySql("SELECT * FROM " + tableInfo.getTableName(), null);
        });
    }
}
```

### 示例 6：使用 DDLOperationsWork 动态建表

```java
@Service
public class DDLService {

    @Autowired
    private OfficeWorkerDao baseDao;

    public void createNewTable(SimpleTableInfo tableInfo) {
        DDLOperationsWork ddlWork = new DDLOperationsWork(baseDao);
        ddlWork.createTable(tableInfo);
    }

    public void addColumn(String tableName, SimpleTableField newField) {
        DDLOperationsWork ddlWork = new DDLOperationsWork(baseDao);
        ddlWork.addColumn(tableName, newField);
    }
}
```

---

## 设计说明

1. **不依赖 Hibernate**：整个模块仅使用 JPA 注解作为元数据标记，ORM 映射由 centit-database 的 `OrmUtils`/`OrmDaoUtils`/`JpaMetadata` 实现
2. **Spring JDBC 集成**：通过 `@Autowired DataSource` 注入，内部创建 `JdbcTemplate`，利用 Spring 的事务管理能力
3. **版本控制**：实现 `EntityWithVersionTag` 即可自动获得乐观锁，`updateObject`/`deleteObject` 会校验版本号
4. **逻辑删除**：实现 `EntityWithDeleteTag` 即可，`deleteObject` 自动变为 `setDeleted(true)` + `updateObject`
5. **过滤条件**：子类重写 `getFilterField()` 定义自定义查询过滤规则，支持多种预处理方式（LIKE、日期、IN 等）
6. **引用关系**：通过 `@OneToMany` + `@JoinColumn` 定义子表关系，`saveObjectReference` 智能对比增删改
