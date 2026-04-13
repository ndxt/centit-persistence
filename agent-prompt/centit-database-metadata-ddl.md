# centit-database 元数据/JSON DAO/DDL 模块

> Maven: `com.centit.support:centit-database`
> 包名: `com.centit.support.database.metadata`, `com.centit.support.database.jsonmaptable`, `com.centit.support.database.ddl`
> 本文件涵盖 centit-database 模块中 metadata、jsonmaptable、ddl 三个包的所有类。

## 模块概述

- **metadata 包**：定义表/字段/引用的元数据接口与实现，以及数据库元数据读取（支持多种数据库和 PDM 文件）
- **jsonmaptable 包**：基于 Map/JSONObject 的数据库表 CRUD 操作，无需 PO 实体类
- **ddl 包**：DDL 操作抽象，支持建表、删表、增删改列、创建序列、创建视图等

**配套文档**: 核心工具类和 ORM 层见 [centit-database-core.md](centit-database-core.md)

---

# 一、metadata 包 — 元数据模型

## 1. IDatabaseInfo (接口)

数据库基本信息接口，通常由框架的数据库配置服务实现。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/IDatabaseInfo.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getDatabaseCode()` | `String` | 数据库编码 |
| `getDatabaseName()` | `String` | 数据库名称 |
| `getDatabaseUrl()` | `String` | 数据库 URL |
| `getUsername()` | `String` | 用户名 |
| `getPassword()` | `String` | 密码（加密） |
| `getClearPassword()` | `String` | 明文密码（`@JSONField(serialize=false)`，不参与序列化） |
| `getExtProps()` | `Map<String, Object>` | 扩展属性（可存放连接池参数等） |
| `getDBType()` | `DBType` | 默认方法：从 URL 推断 DBType |

---

## 2. TableInfo (接口)

表元数据接口，定义表结构信息和多种默认方法。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/TableInfo.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getTableName()` | `String` | 表名 |
| `getTableLabelName()` | `String` | 表中文名 |
| `getTableComment()` | `String` | 表备注 |
| `getPkName()` | `String` | 主键名称 |
| `getSchema()` | `String` | 模式名 |
| `getTableType()` | `String` | 类型（T=表/V=视图/C=缓存） |
| `getOrderBy()` | `String` | 默认排序 |
| `findFieldByName(String propName)` | `TableField` | 按属性名查找字段 |
| `findFieldByColumn(String columnName)` | `TableField` | 按列名查找字段 |
| `getColumns()` | `List<? extends TableField>` | 所有字段 |
| `getReferences()` | `List<? extends TableReference>` | 引用关系列表 |
| `countPkColumn()` | `int` | 主键列数（默认方法） |
| `getPkFields()` | `List<? extends TableField>` | 主键字段列表（默认方法） |
| `getLzayFields()` | `List<? extends TableField>` | 懒加载字段列表（默认方法） |
| `fetchObjectPk(Map<String, Object> object)` | `Map<String, Object>` | 从 Map 提取主键（默认方法） |
| `fetchObjectPkAsId(Map<String, Object> object)` | `String` | 主键值拼为字符串（默认方法） |
| `parseObjectPkId(String pkId)` | `Map<String, Object>` | 从字符串解析主键（默认方法） |
| `fetchObjectPkAsUrlParams(Map<String, Object> object)` | `String` | 主键值转为 URL 参数（默认方法） |

---

## 3. TableField (接口)

字段元数据接口。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/TableField.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getPropertyName()` | `String` | 属性名（小驼峰） |
| `getFieldType()` | `String` | 框架字段类型（如 STRING, INTEGER） |
| `getJavaType()` | `Class<?>` | Java 类型 |
| `getColumnName()` | `String` | 列名（下划线格式） |
| `getColumnType()` | `String` | 数据库列类型（如 VARCHAR2, NUMBER） |
| `getFieldLabelName()` | `String` | 字段中文名 |
| `getColumnComment()` | `String` | 字段注释 |
| `isMandatory()` | `boolean` | 是否非空 |
| `isPrimaryKey()` | `boolean` | 是否主键 |
| `isLazyFetch()` | `boolean` | 是否懒加载 |
| `getMaxLength()` | `Integer` | 最大长度 |
| `getScale()` | `Integer` | 精度 |
| `getDefaultValue()` | `String` | 默认值 |

---

## 4. TableReference (接口)

表引用（外键）关系接口。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/TableReference.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getReferenceCode()` | `String` | 约束代码 |
| `getReferenceName()` | `String` | 约束名称 |
| `getTableName()` | `String` | 子表名称 |
| `getParentTableName()` | `String` | 父表名称 |
| `getReferenceColumns()` | `Map<String, String>` | 主键→外键字段对应关系 |
| `containColumn(String columnName)` | `boolean` | 是否包含某外键字段 |

---

## 5. SimpleTableInfo (class, implements TableInfo)

`TableInfo` 的标准实现。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/SimpleTableInfo.java`

### 关键方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `findFieldByName(String propName)` | `TableField` | 先按属性名，再按列名查找 |
| `findFieldByColumn(String columnName)` | `TableField` | 先按列名，再按属性名查找 |
| `addColumn(SimpleTableField field)` | `void` | 添加字段 |
| `addReference(SimpleTableReference reference)` | `void` | 添加引用关系 |
| `setColumnAsPrimaryKey(String columnName)` | `void` | 设置某字段为主键 |

### 字段（含 getter/setter）

`tableName`, `tableLabelName`, `tableComment`, `pkName`, `schema`, `tableType`, `orderBy`, `columns` (List), `references` (List)

---

## 6. SimpleTableField (class, implements TableField)

`TableField` 的标准实现。包含 `JavaBeanField` 用于反射操作。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/SimpleTableField.java`

### 关键方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `mapToMetadata()` | `SimpleTableField` | 将数据库列类型映射为框架字段类型 |
| `setColumnType(String columnType)` | `void` | 设置列类型（自动解析长度和精度，如 `VARCHAR2(200)` → maxLength=200） |
| `setObjectField(Field field)` | `void` | 设置 Java 反射 Field |

### 字段（含 getter/setter）

`propertyName`, `fieldType`, `javaType`, `columnName`, `columnType`, `fieldLabelName`, `columnComment`, `mandatory`, `primaryKey`, `lazyFetch`, `maxLength`, `scale`, `defaultValue`

---

## 7. SimpleTableReference (class, implements TableReference)

`TableReference` 的标准实现。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/SimpleTableReference.java`

### 关键方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `addReferenceColumn(String parentColumn, String childColumn)` | `void` | 添加父表→子表字段映射 |
| `fetchChildFk(Object parentObject)` | `Map<String, Object>` | 从父对象获取子表外键值 Map |
| `fetchParentPk(Object childObject)` | `Map<String, Object>` | 从子对象获取父表主键值 Map |
| `setObjectFieldValue(Object object, Object value)` | `void` | 通过反射设置引用属性值 |
| `getObjectFieldValue(Object object)` | `Object` | 通过反射获取引用属性值 |

---

## 8. DatabaseMetadata (接口)

数据库元数据读取接口。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/DatabaseMetadata.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static createDatabaseMetadata(DBType dbType)` | `DatabaseMetadata` | 静态工厂方法：按 DBType 创建对应实现类 |
| `setDBConfig(Connection conn)` | `void` | 设置数据库连接 |
| `getTableMetadata(String tableName)` | `SimpleTableInfo` | 获取表元数据（含字段、主键、外键） |
| `getDBSchema()` | `String` | 获取 Schema |
| `setDBSchema(String schema)` | `void` | 设置 Schema |

### 实现类路由

| DBType | 实现类 | 说明 |
|--------|--------|------|
| MySql, PostgreSql, H2, GBase, ClickHouse, Sqlite, Unknown | `JdbcMetadata` | 通用 JDBC 元数据 |
| Oracle, DM, KingBase, Oscar | `OracleMetadata` | Oracle 系统视图 |
| SqlServer | `SqlSvrMetadata` | SQL Server 系统视图 |
| DB2 | `DB2Metadata` | DB2 系统视图 |

---

## 9. JdbcMetadata (class, implements DatabaseMetadata)

通用 JDBC 元数据读取，通过 `java.sql.DatabaseMetaData` 获取表结构。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/JdbcMetadata.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `listTables(boolean includeViews, String[] tableTypes)` | `List<SimpleTableInfo>` | 列出所有表（可过滤类型） |
| `getTableMetadata(String tableName)` | `SimpleTableInfo` | 获取单个表元数据（含字段、主键、外键） |

---

## 10. OracleMetadata (class, implements DatabaseMetadata)

Oracle 数据库元数据读取，使用 `user_tab_columns`、`user_constraints` 等系统视图。也适用于 DM、KingBase、Oscar。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/OracleMetadata.java`

---

## 11. SqlSvrMetadata (class, implements DatabaseMetadata)

SQL Server 元数据读取，使用 `syscolumns`、`sys.key_constraints`、`sys.foreign_keys` 等系统视图。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/SqlSvrMetadata.java`

---

## 12. DB2Metadata (class, implements DatabaseMetadata)

DB2 元数据读取，使用 `sysibm.systables`、`sysibm.syscolumns`、`sysibm.sysrels` 等系统视图。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/DB2Metadata.java`

---

## 13. PdmReader (class, implements DatabaseMetadata)

从 PowerDesigner PDM 文件解析数据库表结构。使用 dom4j 解析 XML。

**路径**: `centit-database/src/main/java/com/centit/support/database/metadata/PdmReader.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static loadPdmFile(String filePath)` | `PdmReader` | 加载 PDM 文件 |
| `getAllTableCode()` | `List<Pair<String, String>>` | 获取所有表代码和名称列表 |
| `getTableMetadata(String tableCode)` | `SimpleTableInfo` | 获取指定表的完整元数据（含字段、主键、外键） |

---

# 二、jsonmaptable 包 — Map/JSON DAO 层

## 14. JsonObjectDao (接口)

基于 Map/JSONObject 的数据库表操作接口。定义了完整的 CRUD、批量操作、SQL 执行等方法。

**路径**: `centit-database/src/main/java/com/centit/support/database/jsonmaptable/JsonObjectDao.java`

### 核心方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getObjectById(Object keyValue)` | `JSONObject` | 根据主键获取 JSON 对象 |
| `getObjectByProperties(Map<String, Object> properties)` | `JSONObject` | 根据属性条件获取单个对象 |
| `listObjectsByProperties(Map<String, Object> properties)` | `JSONArray` | 根据属性条件列表查询 |
| `listObjectsByProperties(Map<String, Object> properties, int pageNo, int pageSize)` | `JSONArray` | 分页查询（pageNo 从 1 开始） |
| `fetchObjectsCount(Map<String, Object> properties)` | `Long` | 按属性条件计数 |
| `getSequenceNextValue(String sequenceName)` | `Long` | 获取序列下一个值 |
| `saveNewObject(Map<String, Object> object)` | `int` | 保存新对象 |
| `saveNewObjectAndFetchGeneratedKeys(Map<String, Object> object)` | `Map<String, Object>` | 保存并返回自增主键 |
| `updateObject(Map<String, Object> object)` | `int` | 更新对象 |
| `updateObject(Collection<String> fields, Map<String, Object> object)` | `int` | 更新指定字段 |
| `mergeObject(Map<String, Object> object)` | `int` | 合并对象 |
| `mergeObject(Collection<String> fields, Map<String, Object> object)` | `int` | 合并指定字段 |
| `updateObjectsByProperties(Map<String, Object> fieldValues, Map<String, Object> properties)` | `int` | 批量条件更新 |
| `deleteObjectById(Object keyValue)` | `int` | 根据主键删除 |
| `deleteObjectsByProperties(Map<String, Object> properties)` | `int` | 根据属性条件删除 |
| `insertObjectsAsTabulation(List<Map<String, Object>> objects)` | `int` | 批量表格式插入 |
| `deleteObjects(List<Object> keyValues)` | `int` | 批量删除 |
| `deleteObjectsAsTabulation(String propertyName, Object propertyValue)` | `int` | 按属性表格式删除 |
| `deleteObjectsAsTabulation(Map<String, Object> properties)` | `int` | 按属性组合表格式删除 |
| `replaceObjectsAsTabulation(List<Map> newObjects, List<Map> dbObjects)` | `int` | 替换表格数据（智能对比） |
| `replaceObjectsAsTabulation(List<Map> newObjects, String propertyName, Object propertyValue)` | `int` | 替换表格数据（按属性） |
| `replaceObjectsAsTabulation(List<Map> newObjects, Map<String, Object> properties)` | `int` | 替换表格数据（按属性组合） |
| `findObjectsBySql(String sql, Object[] params)` | `List<Object[]>` | SQL 查询 |
| `findObjectsBySql(String sql, Object[] params, int pageNo, int pageSize)` | `List<Object[]>` | 分页 SQL 查询 |
| `findObjectsByNamedSql(String sql, Map<String, Object> params)` | `List<Object[]>` | 命名参数 SQL 查询 |
| `findObjectsByNamedSql(String sql, Map<String, Object> params, int pageNo, int pageSize)` | `List<Object[]>` | 分页命名参数查询 |
| `findObjectsAsJSON(String sql, Object[] params, String[] fieldNames)` | `JSONArray` | SQL 查询返回 JSON |
| `findObjectsAsJSON(String sql, Object[] params, String[] fieldNames, int pageNo, int pageSize)` | `JSONArray` | 分页 SQL 查询返回 JSON |
| `findObjectsByNamedSqlAsJSON(String sql, Map<String, Object> params, String[] fieldNames)` | `JSONArray` | 命名参数查询返回 JSON |
| `findObjectsByNamedSqlAsJSON(String sql, Map<String, Object> params, String[] fieldNames, int pageNo, int pageSize)` | `JSONArray` | 分页命名参数查询返回 JSON |
| `doExecuteSql(String sql)` | `boolean` | 执行 SQL |
| `doExecuteSql(String sql, Object[] params)` | `int` | 执行带参 SQL |
| `doExecuteNamedSql(String sql, Map<String, Object> params)` | `int` | 执行命名参数 SQL |

---

## 15. GeneralJsonObjectDao (abstract class, implements JsonObjectDao, ~1358行)

`JsonObjectDao` 的通用实现。提供几乎所有接口方法的默认实现，以及大量的 SQL 构建静态方法。

**路径**: `centit-database/src/main/java/com/centit/support/database/jsonmaptable/GeneralJsonObjectDao.java`

### 工厂方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static createJsonObjectDao(Connection conn, TableInfo tableInfo)` | `GeneralJsonObjectDao` | 按 DBType 创建对应数据库特化实现 |
| `static createJsonObjectDao(Connection conn)` | `GeneralJsonObjectDao` | 自动识别 DBType（需要已设置 tableInfo） |

### SQL 构建静态方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static buildFieldSql(TableInfo tableInfo, String tableAlias, int fieldType)` | `String` | 构建字段 SQL（fieldType: 1=非lazy, 2=lazy, 3=全部） |
| `static buildSelectSqlWithFields(TableInfo tableInfo, String tableAlias, Collection<String> fields)` | `Pair<String, TableField[]>` | 构建 SELECT 语句，返回 SQL 和字段数组 |
| `static buildFilterSql(TableInfo tableInfo, String tableAlias, Map<String, Object> filterMap)` | `String` | 构建过滤条件 SQL。**支持参数后缀**：`_eq`(等于), `_gt`(大于), `_lt`(小于), `_lk`(LIKE), `_in`(IN), `_nl`(NOT LIKE), `_ni`(NOT IN), `_ne`(不等于), `_ge`(大于等于), `_le`(小于等于), `_is`(IS NULL), `_nn`(NOT NULL) |
| `static buildFilterSqlByPk(TableInfo tableInfo, String tableAlias)` | `String` | 构建主键过滤 SQL |
| `static buildInsertSql(TableInfo tableInfo, Collection<String> fields)` | `String` | 构建 INSERT 语句 |
| `static buildUpdateSql(TableInfo tableInfo, Collection<String> fields)` | `String` | 构建 UPDATE 语句 |
| `static buildCountSqlByProperties(TableInfo tableInfo, Map<String, Object> filterMap)` | `String` | 构建 COUNT 语句 |
| `static buildOrderBySql(TableInfo tableInfo, String tableAlias, Map<String, Object> filterMap)` | `String` | 构建 ORDER BY |
| `static fetchSelfOrderSql(TableInfo tableInfo, Map<String, Object> filterMap)` | `String` | 提取自定义排序 SQL（防 SQL 注入） |
| `static checkHasAllPkColumns(TableInfo tableInfo, Map<String, Object> object)` | `boolean` | 检查 Map 是否包含所有主键 |
| `static checkNeedUpdate(Map<String, Object> newObject, Map<String, Object> oldObject)` | `boolean` | 比较新旧对象判断是否需要更新 |

### 过滤参数后缀说明

`buildFilterSql` 方法中的 filterMap 支持特殊后缀来指定比较方式：

| 后缀 | 示例 key | SQL 效果 |
|------|---------|---------|
| (无后缀) | `userName` | `USER_NAME = :userName` |
| `_eq` | `userName_eq` | `USER_NAME = :userName_eq` |
| `_gt` | `age_gt` | `AGE > :age_gt` |
| `_lt` | `age_lt` | `AGE < :age_lt` |
| `_ge` | `age_ge` | `AGE >= :age_ge` |
| `_le` | `age_le` | `AGE <= :age_le` |
| `_lk` | `userName_lk` | `USER_NAME LIKE :userName_lk` |
| `_nl` | `userName_nl` | `USER_NAME NOT LIKE :userName_nl` |
| `_in` | `status_in` | `STATUS IN (:status_in)` |
| `_ni` | `status_ni` | `STATUS NOT IN (:status_ni)` |
| `_ne` | `status_ne` | `STATUS != :status_ne` |
| `_is` | `status_is` | `STATUS IS NULL` |
| `_nn` | `status_nn` | `STATUS IS NOT NULL` |

---

## 16-22. 数据库特化实现类

以下类均继承 `GeneralJsonObjectDao`（H2 继承 `MySqlJsonObjectDao`），主要覆写 `getSequenceNextValue` 方法。

| 类名 | 路径 | 序列获取 SQL |
|------|------|-------------|
| `OracleJsonObjectDao` | `.../jsonmaptable/OracleJsonObjectDao.java` | `SELECT seqName.NEXTVAL FROM dual` |
| `MySqlJsonObjectDao` | `.../jsonmaptable/MySqlJsonObjectDao.java` | 通过存储过程/模拟表实现 |
| `PostgreSqlJsonObjectDao` | `.../jsonmaptable/PostgreSqlJsonObjectDao.java` | `SELECT nextval('seqName')` |
| `DB2JsonObjectDao` | `.../jsonmaptable/DB2JsonObjectDao.java` | `SELECT NEXT VALUE FOR seqName FROM SYSIBM.SYSDUMMY1` |
| `SqlSvrJsonObjectDao` | `.../jsonmaptable/SqlSvrJsonObjectDao.java` | `SELECT NEXT VALUE FOR seqName` |
| `SqliteJsonObjectDao` | `.../jsonmaptable/SqliteJsonObjectDao.java` | 通过模拟表实现 |
| `H2JsonObjectDao` | `.../jsonmaptable/H2JsonObjectDao.java` | `SELECT seqName.NEXTVAL`（继承 MySqlJsonObjectDao） |

---

# 三、ddl 包 — DDL 操作抽象

## 23. DDLOperations (接口)

DDL 操作接口，定义建表、删表、增删改列、创建序列、创建视图等操作。

**路径**: `centit-database/src/main/java/com/centit/support/database/ddl/DDLOperations.java`

### SQL 生成方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `makeCreateSequenceSql(String sequenceName)` | `String` | 生成创建序列 SQL |
| `makeCreateTableSql(TableInfo tableInfo)` | `String` | 生成建表 SQL |
| `makeTableColumnComments(TableInfo tableInfo, int commentContent)` | `List<String>` | 生成字段注释 SQL |
| `makeDropTableSql(String tableName)` | `String` | 生成删表 SQL |
| `makeAddColumnSql(String tableName, TableField column)` | `String` | 生成添加列 SQL |
| `makeModifyColumnSql(String tableName, TableField oldColumn, TableField newColumn)` | `String` | 生成修改列 SQL |
| `makeDropColumnSql(String tableName, String columnName)` | `String` | 生成删除列 SQL |
| `makeRenameColumnSql(String tableName, String oldColumnName, TableField newColumn)` | `String` | 生成重命名列 SQL |
| `makeReconfigurationColumnSqls(String tableName, String columnName, TableField newColumn)` | `List<String>` | 生成列重构 SQL 列表 |
| `makeCreateViewSql(String selectSql, String viewName)` | `String` | 生成创建视图 SQL |

### 执行方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `createSequence(String sequenceName)` | `void` | 执行创建序列 |
| `createTable(TableInfo tableInfo)` | `void` | 执行建表 |
| `dropTable(String tableName)` | `void` | 执行删表 |
| `addColumn(String tableName, TableField column)` | `void` | 执行添加列 |
| `modifyColumn(String tableName, TableField oldColumn, TableField newColumn)` | `void` | 执行修改列 |
| `dropColumn(String tableName, String columnName)` | `void` | 执行删除列 |
| `renameColumn(String tableName, String oldColumnName, TableField newColumn)` | `void` | 执行重命名列 |
| `reconfigurationColumn(String tableName, String columnName, TableField newColumn)` | `void` | 执行列重构 |

---

## 24. GeneralDDLOperations (abstract class, implements DDLOperations)

DDL 操作的通用基类实现。

**路径**: `centit-database/src/main/java/com/centit/support/database/ddl/GeneralDDLOperations.java`

### 工厂方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static createDDLOperations(DBType dbType)` | `GeneralDDLOperations` | 按 DBType 创建对应实现 |
| `static createDDLOperations(Connection conn)` | `GeneralDDLOperations` | 自动识别 DBType |

### 静态工具方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static checkTableWellDefined(TableInfo tableInfo)` | `Pair<Integer, String>` | 检查表定义是否合法 |
| `static parseDDL(String ddlSql)` | `SimpleTableInfo` | 从 DDL SQL 解析表结构 |

---

## 25-31. 数据库特化 DDL 实现

| 类名 | 路径 | 继承 | 特殊适配 |
|------|------|------|---------|
| `OracleDDLOperations` | `.../ddl/OracleDDLOperations.java` | `GeneralDDLOperations` | 序列:`CREATE SEQUENCE`，修改列:`ALTER TABLE ... MODIFY` |
| `MySqlDDLOperations` | `.../ddl/MySqlDDLOperations.java` | `GeneralDDLOperations` | 字段注释通过 `comment` 子句 |
| `PostgreSqlDDLOperations` | `.../ddl/PostgreSqlDDLOperations.java` | `GeneralDDLOperations` | 修改列:`ALTER TABLE ... ALTER ... TYPE` |
| `DB2DDLOperations` | `.../ddl/DB2DDLOperations.java` | `GeneralDDLOperations` | 序列:`CREATE SEQUENCE ... AS INTEGER` |
| `SqlSvrDDLOperations` | `.../ddl/SqlSvrDDLOperations.java` | `GeneralDDLOperations` | 重命名:`sp_rename` |
| `H2DDLOperations` | `.../ddl/H2DDLOperations.java` | `MySqlDDLOperations` | 序列:`CREATE SEQUENCE` |
| `SqliteDDLOperations` | `.../ddl/SqliteDDLOperations.java` | `GeneralDDLOperations` | 建表:`AUTOINCREMENT`，支持从 Map/JSONArray 动态构建表结构 |

---

# 四、使用示例

### 示例 1：使用代码定义表结构并建表

```java
// 定义表结构
SimpleTableInfo tableInfo = new SimpleTableInfo();
tableInfo.setTableName("TEST_TABLE");

SimpleTableField idField = new SimpleTableField();
idField.setColumnName("ID");
idField.setPropertyType("STRING");
idField.setPrimaryKey(true);
idField.setMaxLength(32);
tableInfo.addColumn(idField);

SimpleTableField nameField = new SimpleTableField();
nameField.setColumnName("USER_NAME");
nameField.setPropertyType("STRING");
nameField.setMaxLength(100);
tableInfo.addColumn(nameField);

// 设置主键
tableInfo.setPkName("ID");
tableInfo.setColumnAsPrimaryKey("ID");

// 建表
DDLOperations ddl = GeneralDDLOperations.createDDLOperations(conn);
ddl.createTable(tableInfo);
```

### 示例 2：使用 JsonObjectDao 操作动态表

```java
// 创建 JSON DAO
GeneralJsonObjectDao jsonDao = GeneralJsonObjectDao.createJsonObjectDao(conn, tableInfo);

// 插入
Map<String, Object> newObj = new HashMap<>();
newObj.put("ID", "001");
newObj.put("USER_NAME", "张三");
jsonDao.saveNewObject(newObj);

// 查询
JSONObject result = jsonDao.getObjectById("001");

// 带过滤条件查询
Map<String, Object> filter = new HashMap<>();
filter.put("USER_NAME_lk", "张");
JSONArray list = jsonDao.listObjectsByProperties(filter);

// 更新
result.put("USER_NAME", "李四");
jsonDao.updateObject(result);

// 删除
jsonDao.deleteObjectById("001");
```

### 示例 3：读取数据库元数据

```java
DatabaseMetadata metadata = DatabaseMetadata.createDatabaseMetadata(DBType.Oracle);
metadata.setDBConfig(conn);
metadata.setDBSchema("MY_SCHEMA");

SimpleTableInfo tableMeta = metadata.getTableMetadata("F_USERINFO");
for (TableField field : tableMeta.getColumns()) {
    System.out.println(field.getColumnName() + " - " + field.getColumnType());
}
```

### 示例 4：从 PDM 文件导入表结构

```java
PdmReader reader = PdmReader.loadPdmFile("/path/to/design.pdm");

// 列出所有表
List<Pair<String, String>> tables = reader.getAllTableCode();

// 获取指定表元数据
SimpleTableInfo tableMeta = reader.getTableMetadata("F_USERINFO");
```

### 示例 5：比较表结构并生成 ALTER SQL

```java
SimpleTableInfo newTable = reader.getTableMetadata("F_USERINFO");
DatabaseMetadata dbMeta = DatabaseMetadata.createDatabaseMetadata(conn);
SimpleTableInfo oldTable = dbMeta.getTableMetadata("F_USERINFO");

DDLOperations ddl = GeneralDDLOperations.createDDLOperations(conn);
List<String> alterSqlList = DDLUtils.makeAlterTableSqlList(newTable, oldTable, DBType.Oracle, ddl);
for (String sql : alterSqlList) {
    System.out.println(sql);
}
```

### 示例 6：SqliteDDLOperations 从 JSON 动态建表

```java
// SqliteDDLOperations 支持从 JSONArray 或 List<Map> 直接创建表
SqliteDDLOperations sqliteDdl = (SqliteDDLOperations) GeneralDDLOperations.createDDLOperations(DBType.Sqlite);
JSONArray sampleData = new JSONArray();
JSONObject row = new JSONObject();
row.put("name", "test");
row.put("age", 20);
sampleData.add(row);
// 可根据数据自动推断表结构
```
