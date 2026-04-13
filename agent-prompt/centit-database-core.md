# centit-database 核心模块 (utils + orm 包)

> Maven: `com.centit.support:centit-database`
> 包名: `com.centit.support.database.utils`, `com.centit.support.database.orm`
> 本文件涵盖 centit-database 模块中 utils 和 orm 两个核心包的所有类。

## 模块概述

centit-database 是整个持久化框架的核心抽象层。utils 包提供数据库类型识别、字段类型映射、SQL 构建与解析、JDBC 底层访问等基础能力；orm 包提供基于 JPA 注解的 ORM 映射和完整 CRUD 操作。

**配套文档**: 元数据模型、JSON Map DAO、DDL 操作见 [centit-database-metadata-ddl.md](centit-database-metadata-ddl.md)

---

# 一、utils 包 — 核心工具类

## 1. DBType (枚举)

数据库类型枚举，定义所有支持的数据库类型及驱动映射、识别、验证查询。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/DBType.java`

### 枚举值

| 枚举值 | 说明 |
|--------|------|
| `Unknown` | 未知 |
| `SqlServer` | Microsoft SQL Server |
| `Oracle` | Oracle |
| `DB2` | IBM DB2 |
| `Access` | Microsoft Access |
| `MySql` | MySQL / MariaDB |
| `H2` | H2 Database |
| `PostgreSql` | PostgreSQL |
| `DM` | 达梦数据库（国产） |
| `KingBase` | 人大金仓（国产） |
| `GBase` | 南大通用（国产） |
| `Oscar` | 神通数据库（国产） |
| `Sqlite` | SQLite |
| `ClickHouse` | ClickHouse |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `isMadeInChina()` | `boolean` | 判断是否为国产数据库（DM, KingBase, GBase, Oscar） |
| `static valueOf(int ordinal)` | `DBType` | 按序号获取 DBType |
| `static mapDBType(String connurl)` | `DBType` | 根据 JDBC URL 识别数据库类型 |
| `static mapDBType(String connurl, DBType defaultType)` | `DBType` | 带默认值的数据库类型识别 |
| `static mapDBType(Connection conn)` | `DBType` | 从 Connection 获取数据库类型 |
| `static mapDialectToDBType(String dialectName)` | `DBType` | 从 Hibernate 方言名识别数据库类型 |
| `static allValues()` | `Set<DBType>` | 返回所有数据库类型集合 |
| `static getDbDriver(DBType dt)` | `String` | 获取对应数据库的 JDBC 驱动类名 |
| `static setDbDriver(DBType dt, String driverClassName)` | `void` | 设置数据库驱动类名 |
| `static getDBTypeName(DBType type)` | `String` | 获取数据库类型字符串名称 |
| `static getDBValidationQuery(DBType type)` | `String` | 获取数据库连接验证查询 SQL |

---

## 2. FieldType (abstract class, 工具类)

字段类型映射工具，定义统一的字段类型常量，提供 Java 类型、数据库列类型和框架内部字段类型之间的相互转换。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/FieldType.java`

### 字段类型常量

| 常量 | 说明 |
|------|------|
| `VOID` | 空 |
| `IDENTITY` | 自增标识 |
| `STRING` | 字符串 |
| `INTEGER` | 整数 |
| `FLOAT` | 浮点数 |
| `MONEY` | 金额 |
| `DOUBLE` | 双精度 |
| `LONG` | 长整数 |
| `BOOLEAN` | 布尔 |
| `DATE` | 日期 |
| `DATETIME` | 日期时间 |
| `TIMESTAMP` | 时间戳 |
| `FILE_ID` | 文件ID |
| `ENUM_NAME` | 枚举名 |
| `TEXT` | 大文本 |
| `BYTE_ARRAY` | 字节数组 |
| `FILE` | 文件 |
| `JSON_OBJECT` | JSON 对象 |
| `OBJECT_LIST` | 对象列表 |

### 名称转换方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static mapClassName(String columnName)` | `String` | 列名转大驼峰类名（如 `USER_NAME` → `UserName`） |
| `static mapPropName(String columnName)` | `String` | 列名转小驼峰属性名（如 `USER_NAME` → `userName`） |
| `static mapToHumpName(String name, boolean firstUpper, boolean underscoreAsSplitter)` | `String` | 通用驼峰转换 |
| `static humpNameToColumn(String humpName, boolean toUpper)` | `String` | 驼峰名转下划线列名 |

### 数据库列类型映射方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static mapToOracleColumnType(String fieldType)` | `String` | 映射到 Oracle 列类型 |
| `static mapToMySqlColumnType(String fieldType)` | `String` | 映射到 MySQL 列类型 |
| `static mapToPostgreSqlColumnType(String fieldType)` | `String` | 映射到 PostgreSQL 列类型 |
| `static mapToSqlServerColumnType(String fieldType)` | `String` | 映射到 SQL Server 列类型 |
| `static mapToDB2ColumnType(String fieldType)` | `String` | 映射到 DB2 列类型 |
| `static mapToClickHouseColumnType(String fieldType)` | `String` | 映射到 ClickHouse 列类型 |
| `static mapToGBaseColumnType(String fieldType)` | `String` | 映射到 GBase 列类型 |
| `static mapToSqliteColumnType(String fieldType)` | `String` | 映射到 SQLite 列类型 |
| `static mapToDatabaseType(String fieldType, DBType dbType)` | `String` | 按 DBType 路由到对应数据库列类型 |

### 类型互转方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static mapToJavaType(String columnTypeName, int columnType)` | `Class<?>` | 列类型名转 Java 类型 |
| `static mapToJavaType(int sqlType)` | `Class<?>` | java.sql.Types 常量转 Java 类型 |
| `static mapToFieldType(String columnTypeName, int columnType)` | `String` | 列类型名转框架字段类型 |
| `static mapToFieldType(int sqlType)` | `String` | java.sql.Types 常量转框架字段类型 |
| `static mapToFieldType(Class<?> javaType)` | `String` | Java 类型转框架字段类型 |
| `static getAllTypeMap()` | `Map<String, String>` | 获取所有字段类型及中文描述 |

---

## 3. QueryUtils (abstract class, 工具类, ~1800行)

SQL 查询构建工具类。提供 SQL 语句解析、分页查询 SQL 生成、参数预处理、命名参数转换、SQL 模板解析等核心功能。是框架中最核心的 SQL 处理类。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/QueryUtils.java`

### 参数预处理常量

这些常量用于 `scalarPretreatParameter()` 方法和查询过滤机制中的参数预处理：

| 常量 | 值 | 说明 |
|------|-----|------|
| `SQL_PRETREAT_NO_PARAM` | 无参数预处理 | 不进行任何转换 |
| `SQL_PRETREAT_LIKE` | LIKE 匹配 | 将参数值包装为 `%value%` |
| `SQL_PRETREAT_STARTWITH` | 前缀匹配 | `value%` |
| `SQL_PRETREAT_ENDWITH` | 后缀匹配 | `%value` |
| `SQL_PRETREAT_DATE` | 日期 | 转为日期格式 |
| `SQL_PRETREAT_DATETIME` | 日期时间 | 转为日期时间格式 |
| `SQL_PRETREAT_NEXT_DAY` | 次日 | 日期+1天 |
| `SQL_PRETREAT_NEXT_MONTH` | 次月 | 日期+1月 |
| `SQL_PRETREAT_NEXT_YEAR` | 次年 | 日期+1年 |
| `SQL_PRETREAT_NEXT_WEEK` | 次周 | 日期+7天 |
| `SQL_PRETREAT_TRUNC_DAY` | 截断到日 | 去掉时分秒 |
| `SQL_PRETREAT_TRUNC_MONTH` | 截断到月 | 去掉日月以下 |
| `SQL_PRETREAT_TRUNC_YEAR` | 截断到年 | 去掉月以下 |
| `SQL_PRETREAT_TRUNC_WEEK` | 截断到周 | 周一 |
| `SQL_PRETREAT_DATESTR` | 日期字符串 | 日期转字符串 |
| `SQL_PRETREAT_DIGIT` | 数字提取 | 提取数字字符 |
| `SQL_PRETREAT_UPPERCASE` | 大写 | 转大写 |
| `SQL_PRETREAT_LOWERCASE` | 小写 | 转小写 |
| `SQL_PRETREAT_NUMBER` | 数字 | 转为数字 |
| `SQL_PRETREAT_QUOTASTR` | 引号字符串 | 添加引号 |
| `SQL_PRETREAT_INTEGER` | 整数 | 转为整数 |
| `SQL_PRETREAT_LONG` | 长整数 | 转为长整数 |
| `SQL_PRETREAT_FLOAT` | 浮点数 | 转为浮点数 |
| `SQL_PRETREAT_STRING` | 字符串 | 转为字符串 |
| `SQL_PRETREAT_SPLITFORIN` | IN分割 | 将逗号分隔字符串展开为 IN 子句 |
| `SQL_PRETREAT_CREEPFORIN` | IN展开 | 将数组参数展开为 IN 子句的多个 `?` |
| `SQL_PRETREAT_LOOP` | 循环 | 循环展开（类似 SPLITFORIN） |
| `SQL_PRETREAT_LOOP_WITH_OR` | OR循环 | 用 OR 连接展开 |
| `SQL_PRETREAT_INPLACE` | 原地替换 | 直接替换参数值 |
| `SQL_PRETREAT_ESCAPE_HTML` | HTML转义 | 转义 HTML 特殊字符 |

### SQL 安全与格式化方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static buildStringForQuery(String value)` | `String` | 将字符串包装为 SQL 安全格式 `'value'`（处理单引号转义） |
| `static buildDateStringForQuery(Date date)` | `String` | 日期转 SQL 日期字符串 |
| `static buildDatetimeStringForQuery(Date date)` | `String` | 日期时间转 SQL 字符串 |
| `static buildDateStringForOracle(Date date)` | `String` | Oracle 日期格式转换 |
| `static getMatchString(String value)` | `String` | 将字符串转为 LIKE 匹配模式 |
| `static replaceMatchParams(Map<String, Object> filterMap, Collection<String> fields)` | `int` | 批量替换 LIKE 参数 |

### SQL 解析方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static hasOrderBy(String sql)` | `boolean` | 判断 SQL 是否含 ORDER BY |
| `static removeOrderBy(String sql)` | `String` | 移除 ORDER BY 子句 |
| `static getGroupByField(String sql)` | `String` | 提取 GROUP BY 字段 |
| `static splitSqlByFields(String sql)` | `List<String>` | 将 SQL 按 select/from 分段 |
| `static getSqlFiledNames(String sql)` | `List<String>` | 获取 SQL 查询的字段名列表 |
| `static splitSqlFieldNames(String sql)` | `List<String>` | 拆分 SQL 字段名 |
| `static trimSqlOrderByField(String orderBySql)` | `String` | 过滤 ORDER BY 中可能的注入 |
| `static extraSqlFieldNamePieceMap(String sql)` | `List<Pair<String, String>>` | 提取 SQL 字段名及表达式 |
| `static extraTables(String sql)` | `Map<String, String>` | 提取 SQL 中的表名和别名 |

### 计数 SQL 构建

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static buildGetCountSQLByReplaceFields(String querySql)` | `String` | 构建计数查询（字段替换方式：替换 select ... 为 select count(*)） |
| `static buildGetCountSQLBySubSelect(String querySql)` | `String` | 构建计数查询（子查询方式：select count(*) from (原sql)） |
| `static buildGetCountSQL(String querySql)` | `String` | 智能构建计数查询（自动选择替换或子查询方式） |
| `static buildGetCountHQL(String hql)` | `String` | 构建 HQL 计数查询 |

### 分页 SQL 构建

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static buildPostgreSqlLimitQuerySQL(String sql, int startPos, int maxSize, boolean hasOrderBy)` | `String` | PostgreSQL 分页 SQL（LIMIT/OFFSET） |
| `static buildMySqlLimitQuerySQL(String sql, int startPos, int maxSize, boolean hasOrderBy)` | `String` | MySQL 分页 SQL（LIMIT offset, size） |
| `static buildOracleLimitQuerySQL(String sql, int startPos, int maxSize, boolean hasOrderBy)` | `String` | Oracle 分页 SQL（ROWNUM） |
| `static buildDB2LimitQuerySQL(String sql, int startPos, int maxSize)` | `String` | DB2 分页 SQL（ROW_NUMBER() OVER()） |
| `static buildSqlServerLimitQuerySQL(String sql, int startPos, int maxSize)` | `String` | SQL Server 分页 SQL（OFFSET ... FETCH） |
| `static buildLimitQuerySQL(String sql, int startPos, int maxSize, boolean hasOrderBy, DBType dbType)` | `String` | **统一分页 SQL 生成**（按 DBType 自动路由到对应方法） |

### 命名参数处理

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static transNamedParamSqlToParamSql(String namedParamSql)` | `LeftRightPair<String, List<String>>` | 将命名参数 SQL（`:paramName`）转为位参 SQL（`?`），返回转换后的 SQL 和参数名列表 |
| `static getSqlNamedParameters(String sql)` | `List<String>` | 获取 SQL 中所有命名参数（`:paramName` 格式） |
| `static getSqlTemplateParameters(String sql)` | `Set<String>` | 获取 SQL 模板中的所有参数（含条件模板 `:n{...}` 格式） |

### 参数预处理

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static scalarPretreatParameter(String pretreat, Object value)` | `Object` | 对参数进行预处理（日期、LIKE、大小写、数字、IN 展开等）。pretreat 为上述常量之一 |

### 参数驱动查询 — translateQuery

这是 QueryUtils 中最复杂也最强大的功能。`translateQuery` 方法支持在 SQL 语句中嵌入特殊占位符，根据运行时参数的值动态决定哪些条件片段出现在最终 SQL 中。适用于前端查询表单中参数不确定的场景，避免大量 if-else 拼接 SQL。

#### 方法签名

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static translateQuery(String queryStatement, Collection<String> filters, Object paramsMap, boolean isUnion)` | `QueryAndNamedParams` | 完整版：同时支持方法一（外置过滤）和方法二（内置条件） |
| `static translateQuery(String queryStatement, Object paramsMap)` | `QueryAndNamedParams` | 简化版：仅使用方法二（内置条件），无外部过滤器 |
| `static translateQuery(Map<String, String> tableMap, Collection<String> filters, Object paramsMap, boolean isUnion)` | `QueryAndNamedParams` | 仅生成外置过滤条件片段（不嵌入 SQL 语句） |
| `static translateQueryPiece(String queryPiece, IFilterTranslate translate)` | `QueryAndNamedParams` | 转换单个内置条件片段（`[...]` 内的内容） |
| `static translateQueryFilter(String filter, IFilterTranslate translate)` | `QueryAndNamedParams` | 转换单个外置过滤条件 |
| `static translateQueryFilter(Collection<String> filters, IFilterTranslate translate, boolean isUnion)` | `QueryAndNamedParams` | 转换多个外置过滤条件（用 AND/OR 连接） |

**参数说明**：
- `queryStatement` — 包含占位符的 SQL 模板
- `filters` — 外置过滤条件集合（方法一）
- `paramsMap` — 查询参数，Map<String, Object> 或 VariableTranslate 实现
- `isUnion` — 同一占位符中多个匹配条件的连接方式：`true`=OR，`false`=AND

#### 方法一：外置过滤条件（`{...}` 占位符）

过滤条件独立存放在 `filters` 集合中，SQL 中用 `{表名:别名,...}` 占位符引用。适用于权限过滤等复用场景。

**占位符语法**：`{TableName:alias,TableName2:alias2,...}`
- 放在 WHERE 子句中
- 可加 `required` 关键字：`{required TableName:alias}` — 如果没有匹配的过滤条件则生成 `and 0=1`
- 多个占位符之间用 AND 连接
- 同一占位符内多个匹配条件用 OR/AND 连接（由 `isUnion` 参数决定）
- 没有匹配任何过滤条件的占位符自动消失

**过滤条件语法**：`[表名.字段] 运算符 {参数表达式}`
- `[表名.字段]` — 字段引用，会被替换为 `别名.字段`
- `{参数表达式}` — 参数引用，格式见下方"参数表达式语法"

**工作原理**：遍历 filters，将 filter 中的 `[表名.字段]` 与占位符中的 `TableName:alias` 匹配。表名匹配且参数在 paramsMap 中存在时，该过滤条件被选中，其中的字段名替换为 `别名.字段`，参数替换为命名参数 `:paramAlias`。

**示例**：

```java
List<String> filters = new ArrayList<>();
filters.add("[table1.c] like {p1.1:ps}");           // (1) table1.c LIKE :ps，参数名p1.1，别名ps
filters.add("[table1.b] = {p5}");                   // (2) table1.b = :p5
filters.add("[table4.b] = {p4}");                   // (3) table4.b = :p4
filters.add("([table2.f]={p2} and [table3.f]={p3})");// (4) 组合条件

Map<String, Object> paramsMap = new HashMap<>();
paramsMap.put("p1.1", "1");
paramsMap.put("p2", "3");

String sql = "select t1.a,t2.b,t3.c from table1 t1,table2 t2,table3 t3 "
           + "where 1=1 {table1:t1} order by 1,2";

// {table1:t1} 匹配表名为 table1 的过滤器 → 选中 (1) 和 (2)
// (2) 的参数 p5 不在 paramsMap 中 → 排除
// 最终结果: ... where 1=1 and t1.c like :ps order by 1,2
```

**复杂示例**：

```java
String sql = "select t1.a,t2.b,t3.c from table1 t1,table2 t2,table3 t3 "
           + "where 1=1 {table1:t1}{table9:t1}{table2:t2,table3:t3,table4:t1} order by 1,2";
paramsMap.put("p3", "5");
paramsMap.put("p4", "7");

// {table1:t1} → 匹配 (1) → t1.c like :ps
// {table9:t1} → 无匹配 → 自动消失
// {table2:t2,table3:t3,table4:t1} → 匹配 (3)(4) → (t1.b = :p4 or (t2.f=:p2 and t3.f=:p3))
// 最终结果: ... where 1=1 and t1.c like :ps and (t1.b = :p4 or (t2.f=:p2 and t3.f=:p3)) order by 1,2
```

#### 方法二：内置条件语句（`[...]` 占位符）

条件直接内嵌在 SQL 语句中，根据参数值动态决定是否出现在最终 SQL 中。适用于前端查询条件值自动配置查询语句的场景。

**完整语法**：`[(逻辑表达式)(参数列表)|SQL片段]`

**工作流程**：
1. 计算逻辑表达式的值（表达式可引用 paramsMap 中的参数）
2. 值为 false / 0 / null → 整个占位符消失
3. 值为 true / 非零 → 将 `|` 后面的 SQL 片段加入查询，并将参数列表中存在于 paramsMap 的参数加入返回的参数集

**逻辑表达式**：
- 直接使用标识符引用参数（如 `p2`），自动从 paramsMap 取值，null 视为 false，非 null 视为 true
- 不符合标识符格式的参数名用 `${...}` 包裹（如 `${p1.1}`）
- 支持运算符：`>`, `<`, `>=`, `<=`, `==`, `!=`, `&&`, `||`
- 支持内置函数：`isNotEmpty()`, `isEmpty()` 等（来自 compiler 模块的 VariableFormula）

**参数列表**（可选）：逗号分隔的参数定义，每个参数格式为：

| 格式 | 说明 |
|------|------|
| `paramName` | 仅用于判断是否存在，不添加到查询参数 |
| `paramName:paramAlias` | 判断是否存在，并将值以 paramAlias 别名添加到查询参数 |
| `:paramAlias` | `paramName == paramAlias` 时的简写 |
| `paramName:(pretreat)paramAlias` | 带预处理的完整格式 |

**注意**：`paramName:` (冒号后为空) 是**不允许**的。

**简化语法**：`[参数1,:参数2,参数3:别名|SQL片段]`

当所有列出的参数在 paramsMap 中都存在时，SQL 片段才会加入查询。参数前有 `:` 的会同时添加到查询参数。

**占位符可以出现在 SQL 的任何位置**（SELECT、FROM、WHERE 均可），不仅限于 WHERE 子句。

**示例 1 — 完整语法**：

```java
Map<String, Object> paramsMap = new HashMap<>();
paramsMap.put("p1.1", "5");
paramsMap.put("p2", "3");

String sql = "select [(${p1.1} > 2 && p2 > 2)|t1.a,] t2.b,t3.c "
           + "from [(${p1.1} > 2 && p2 > 2)| table1 t1,] table2 t2,table3 t3 "
           + "where 1=1 [(${p1.1} > 2 && p2 > 2)(p1.1:ps)| and t1.a=:ps]"
           + "[(isNotEmpty(${p1.1}) && isNotEmpty(p2) && isNotEmpty(p3))(p2,p3:px)"
           + "| and (t2.b > :p2 or t3.c > :px)] order by 1,2";

// p1.1=5 (>2 ✓), p2=3 (>2 ✓), p3=null (isNotEmpty ✗)
// SELECT中的占位符: ${p1.1}>2 && p2>2 → true → 保留 "t1.a,"
// FROM中的占位符: 同上 → 保留 "table1 t1,"
// WHERE第一个占位符: 同上 + 参数p1.1存在 → 保留 "and t1.a=:ps"，添加参数 ps=5
// WHERE第二个占位符: isNotEmpty(p3) → false → 消失
// 最终结果: select t1.a, t2.b,t3.c from table1 t1, table2 t2,table3 t3
//           where 1=1 and t1.a=:ps order by 1,2
```

**示例 2 — 简化语法**：

```java
// 简化语法：只有参数都存在时才保留SQL片段
String sql = "select t2.b,t3.c from table2 t2,table3 t3 "
           + "where 1=1 [p1.1,:p2,p3:px| and (t2.b > :p2 or t3.c > :px)] order by 1,2";

// p1.1=5(存在✓), p2=3(存在✓+添加参数), p3=null(不存在✗) → 整个片段消失
// 最终结果: select t2.b,t3.c from table2 t2,table3 t3 where 1=1 order by 1,2
```

**示例 3 — 混合使用方法一和方法二**：

```java
List<String> filters = new ArrayList<>();
filters.add("[table1.c] like {p1:ps}");

Map<String, Object> paramsMap = new HashMap<>();
paramsMap.put("p1", "test");
paramsMap.put("p2", "3");

String sql = "select * from table1 t1 "
           + "where 1=1 {table1:t1}[p2| and t1.d > :p2] order by 1";

// 方法一 {table1:t1} → 匹配filter → "and t1.c like :ps"
// 方法二 [p2|...] → p2存在 → "and t1.d > :p2"
// 最终结果: select * from table1 t1 where 1=1 and t1.c like :ps and t1.d > :p2 order by 1
```

#### 参数表达式语法

在 `{...}` 中引用参数，有以下格式：

| 格式 | 示例 | 说明 |
|------|------|------|
| `{paramName}` | `{p2}` | 参数名 = 别名，直接作为命名参数 `:p2` |
| `{paramName:paramAlias}` | `{p1.1:ps}` | 参数名和别名不同，生成 `:ps` |
| `{(pretreat)paramName}` | `{(LIKE)p2}` | 带预处理，对参数值进行转换后使用 |
| `{paramName:(pretreat)paramAlias}` | `{p1:(LIKE)ps}` | 完整格式：参数名 + 预处理 + 别名 |

**预处理方式**：使用上述"参数预处理常量"中的值，如 `LIKE`(模糊匹配)、`DATE`(日期)、`SPLITFORIN`(IN分割)、`CREEPFORIN`(IN展开)、`INPLACE`(原地替换)、`UPPERCASE`(大写) 等。

**特殊处理**：
- `CREEPFORIN` / `SPLITFORIN`：数组参数自动展开为 `IN (?, ?, ...)` 形式
- `INPLACE`：参数值直接嵌入 SQL（非参数绑定），会进行 SQL 注入防护
- `LOOP_WITH_OR`：数组参数展开为多个 OR 条件

#### 设计说明

1. **方法一 vs 方法二**：方法一将过滤条件外置，适合不同查询复用（典型场景：权限过滤）；方法二条件内嵌，编写更优雅灵活（典型场景：前端查询表单）。两种方法可以混合使用
2. **不支持嵌套**：占位符内不能再嵌套占位符。如需嵌套效果，可调用 `translateQuery` 两次
3. **不做合法性检查**：queryStatement 可以仅仅是一个 `{}` 占位符，用于获取查询条件片段后手动拼接到自定义 SQL 中
4. **参数值判断**：参数值为 null 或空字符串时，该参数相关的条件自动排除（实现"前端不填则不筛选"的逻辑）

---

## 4. DatabaseAccess (abstract class, 工具类, ~960行)

核心数据库访问类。基于原生 JDBC 提供 SQL 执行、查询、分页查询、结果集转换、存储过程调用等功能。所有方法都需要传入 `Connection` 参数。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/DatabaseAccess.java`

### SQL 执行

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static doExecuteSql(Connection conn, String sql)` | `boolean` | 执行无参 SQL（DDL 等） |
| `static doExecuteSql(Connection conn, String sql, Object[] params)` | `int` | 执行带参 SQL（INSERT/UPDATE/DELETE），返回影响行数 |
| `static doExecuteNamedSql(Connection conn, String sql, Map<String, Object> params)` | `int` | 执行命名参数 SQL |
| `static setQueryStmtParameters(PreparedStatement stmt, Object[] params)` | `void` | 设置预编译参数（支持日期、CLOB、BLOB 等类型） |

### 存储过程

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static callFunction(Connection conn, String functionName, int sqlType, Object... params)` | `Object` | 调用数据库函数（存储过程），返回指定类型的输出 |
| `static callProcedure(Connection conn, String procedureName, Object... params)` | `boolean` | 执行存储过程 |

### 查询 — 返回 JSONArray

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static fetchResultSetToJSONArray(ResultSet rs, String[] fieldNames)` | `JSONArray` | ResultSet 转 JSONArray |
| `static fetchResultSetRowToJSONObject(ResultSet rs)` | `JSONObject` | ResultSet 单行转 JSONObject |
| `static findObjectsAsJSON(Connection conn, String sql, Object[] params, String[] fieldNames)` | `JSONArray` | 查询返回 JSONArray |
| `static findObjectsAsJSON(Connection conn, String sql, Object[] params, String[] fieldNames, int pageNo, int pageSize)` | `JSONArray` | 分页查询返回 JSONArray（pageNo 从 1 开始） |
| `static getObjectAsJSON(Connection conn, String sql, Object[] params)` | `JSONObject` | 查询单个对象为 JSON |

### 查询 — 返回 List<Object[]>

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static findObjectsBySql(Connection conn, String sql, Object[] params)` | `List<Object[]>` | 查询返回对象列表 |
| `static findObjectsBySql(Connection conn, String sql, Object[] params, int pageNo, int pageSize)` | `List<Object[]>` | 分页查询（pageNo 从 1 开始） |
| `static findObjectsByNamedSql(Connection conn, String sql, Map<String, Object> params)` | `List<Object[]>` | 命名参数查询 |
| `static getSingleRow(Connection conn, String sql, Map<String, Object> params)` | `Object[]` | 查询单行 |
| `static getScalarObjectQuery(Connection conn, String sql, Map<String, Object> params)` | `Object` | 标量查询（返回第一行第一列） |
| `static queryTotalRows(Connection conn, String sql, Object[] params)` | `Long` | 查询总记录数 |

### LOB 处理

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static fetchClobString(Clob clob)` | `String` | 读取 CLOB 为字符串 |
| `static fetchBlobBytes(Blob blob)` | `byte[]` | 读取 BLOB 为字节数组 |
| `static fetchBlobAsBase64(Blob blob)` | `String` | BLOB 转 Base64 字符串 |

---

## 5. PageDesc (class, Serializable)

分页描述对象，封装分页参数。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/PageDesc.java`

### 字段（含 getter/setter）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `totalRows` | `int` | 0 | 总行数 |
| `pageSize` | `int` | 20 | 每页大小 |
| `pageNo` | `int` | 1 | 页码（从 1 开始） |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static createNotPaging()` | `PageDesc` | 静态工厂：创建不分页的 PageDesc |
| `getRowStart()` | `int` | 当前页起始行索引（0-based） |
| `getRowEnd()` | `int` | 当前页结束行索引 |
| `noPaging(int totalRows)` | `void` | 设置为不分页模式（设 pageSize = totalRows） |

---

## 6. QueryAndNamedParams (class, Serializable)

封装命名参数查询语句及其参数 Map。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/QueryAndNamedParams.java`

### 字段（含 getter）

| 字段 | 类型 | 说明 |
|------|------|------|
| `queryStmt` | `String` | SQL/HQL 语句 |
| `params` | `Map<String, Object>` | 命名参数 Map |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `getQuery()` | `String` | 获取查询语句 |
| `getParams()` | `Map<String, Object>` | 获取参数 Map |
| `addParam(String name, Object value)` | `QueryAndNamedParams` | 链式添加参数 |
| `addAllParams(Map<String, Object> params)` | `QueryAndNamedParams` | 链式批量添加参数 |

---

## 7. QueryAndParams (class, Serializable)

封装位参查询语句及其参数数组。提供命名参数到位参的转换和 IN 语句数组参数扩展。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/QueryAndParams.java`

### 字段（含 getter/setter）

| 字段 | 类型 | 说明 |
|------|------|------|
| `queryStmt` | `String` | SQL 语句 |
| `params` | `Object[]` | 参数数组 |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `creepArrayParamForInQuery(String paramName, Object[] arrayParam)` | `QueryAndParams` | 将数组参数展开为 IN 语句的多个 `?` 占位符 |
| `static createFromQueryAndNamedParams(String queryStmt, Map<String, Object> namedParams)` | `QueryAndParams` | 将命名参数查询转为位参查询（处理数组参数的 IN 展开） |

---

## 8. QueryLogUtils (abstract class, 工具类)

SQL 日志打印工具。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/QueryLogUtils.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static setJdbcShowSql(boolean showSql)` | `void` | 设置是否显示 SQL |
| `static setUserLog4j(boolean useLog4j)` | `void` | 设置是否使用 Log4j |
| `static printSql(Logger logger, String sql)` | `void` | 打印 SQL 语句 |
| `static printSql(Logger logger, String sql, Object param)` | `void` | 打印 SQL 语句和参数 |

---

## 9. DDLUtils (abstract class, 工具类)

通过比较新旧表结构生成 ALTER TABLE SQL。

**路径**: `centit-database/src/main/java/com/centit/support/database/utils/DDLUtils.java`

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static makeAlterTableSqlList(TableInfo newTable, TableInfo oldTable, DBType dbType, DDLOperations ddl)` | `List<String>` | 比较新旧表结构，生成 DDL 变更 SQL 列表 |

---

# 二、orm 包 — ORM 映射层

## 10. @ValueGenerator (注解)

字段值生成器注解，定义字段值如何自动生成。

**路径**: `centit-database/src/main/java/com/centit/support/database/orm/ValueGenerator.java`

**元注解**: `@Target({METHOD, FIELD})`, `@Retention(RUNTIME)`

### 注解属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `strategy()` | `GeneratorType` | `AUTO` | 生成策略 |
| `occasion()` | `GeneratorTime` | `NEW_UPDATE` | 生成时机 |
| `condition()` | `GeneratorCondition` | `IFNULL` | 生成条件 |
| `value()` | `String` | `""` | 生成参数（如序列名、常量值等） |

---

## 11. GeneratorType (枚举)

值生成策略类型。

**路径**: `centit-database/src/main/java/com/centit/support/database/orm/GeneratorType.java`

| 枚举值 | 说明 |
|--------|------|
| `AUTO` | 自动（根据字段类型选择） |
| `SEQUENCE` | 数据库序列（value 指定序列名） |
| `UUID` | 32 位 UUID |
| `UUID22` | 22 位 UUID（Base64 编码） |
| `SNOWFLAKE` | 雪花算法 ID |
| `CONSTANT` | 固定常量（value 指定值） |
| `FUNCTION` | 函数调用（value 指定表达式） |
| `SERIAL_NO` | 序列号 |
| `RANDOM_ID` | 随机 ID |
| `RANDOM_LOW_STRING_ID` | 随机小写字符串 ID |
| `TIME_SEQUENCE` | 时间序列 |
| `SUB_ORDER` | 子订单序号（已废弃） |

---

## 12. GeneratorTime (枚举)

值生成时机。

**路径**: `centit-database/src/main/java/com/centit/support/database/orm/GeneratorTime.java`

| 枚举值 | 说明 |
|--------|------|
| `NEW` | 仅新增时生成 |
| `UPDATE` | 仅更新时生成 |
| `READ` | 读取时生成 |
| `NEW_UPDATE` | 新增和更新时都生成 |
| `ALWAYS` | 总是生成 |

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `matchTime(GeneratorTime occasion)` | `boolean` | 判断当前时机是否匹配 |

---

## 13. GeneratorCondition (枚举)

值生成条件。

| 枚举值 | 说明 |
|--------|------|
| `IFNULL` | 仅在字段值为 null 时生成 |
| `ALWAYS` | 总是生成（覆盖已有值） |

---

## 14. JpaMetadata (abstract class, 工具类)

通过 JPA 注解（`@Table`, `@Column`, `@Id`, `@OneToOne` 等）解析实体类，构建 `TableMapInfo` 对象。使用 ConcurrentHashMap 缓存元数据。

**路径**: `centit-database/src/main/java/com/centit/support/database/orm/JpaMetadata.java`

**使用的 JPA 注解**: `jakarta.persistence.Table`, `Column`, `Id`, `OneToOne`, `OneToMany`, `ManyToOne`, `ManyToMany`, `JoinColumn`, `JoinTable`, `EmbeddedId`, `Basic`

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static fetchTableMapInfo(Class<?> clazz)` | `TableMapInfo` | 获取（或解析并缓存）类的表映射信息 |
| `static translatePropertyNameToColumnName(Class<?> clazz, String propertyName)` | `String` | 属性名转列名（如 `userName` → `USER_NAME`） |
| `static translatePropertyNameToColumnName(String classNameAndProp)` | `String` | 通过 `"类全名.属性名"` 格式转换 |
| `static translateSqlPropertyToColumn(TableMapInfo mapInfo, String sql, String tableAlias)` | `String` | 将 SQL 中的属性名替换为数据库列名 |

---

## 15. TableMapInfo (class, extends SimpleTableInfo)

表与 Java 对象的映射信息，扩展 SimpleTableInfo 增加值生成器、嵌入式 ID、JavaBean 字段操作等。

**路径**: `centit-database/src/main/java/com/centit/support/database/orm/TableMapInfo.java`

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `addValueGenerator(String fieldName, ValueGenerator generator)` | `TableMapInfo` | 添加字段值生成器（链式调用） |
| `hasGeneratedKeys()` | `boolean` | 是否有自增主键 |
| `fetchGeneratedKey()` | `SimpleTableField` | 获取自增主键字段 |
| `appendOrderBy(SimpleTableField field, String direction)` | `void` | 追加排序字段 |
| `getObjectFieldValue(Object object, SimpleTableField field)` | `Object` | 通过反射获取对象字段值（支持 Map 和 POJO） |
| `setObjectFieldValue(Object object, SimpleTableField field, Object value)` | `void` | 通过反射设置对象字段值 |
| `fetchObjectPk(Object object)` | `Map<String, Object>` | 获取对象主键值 Map |

---

## 16. OrmUtils (abstract class, 工具类)

ORM 核心工具。提供对象属性与数据库字段的转换、ResultSet 到 Java 对象的映射、值生成器执行等功能。

**路径**: `centit-database/src/main/java/com/centit/support/database/orm/OrmUtils.java`

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static prepareObjectForInsert(T object, TableMapInfo mapInfo, JsonObjectDao dao)` | `T` | 插入前执行值生成器（NEW 时机的生成器） |
| `static prepareObjectForUpdate(T object, TableMapInfo mapInfo, JsonObjectDao dao)` | `T` | 更新前执行值生成器（UPDATE 时机的生成器） |
| `static prepareObjectForMerge(T object, TableMapInfo mapInfo, JsonObjectDao dao)` | `T` | 合并前执行值生成器 |
| `static fetchObjectField(Object object)` | `Map<String, Object>` | 获取对象所有字段（支持 Map 和 POJO） |
| `static fetchObjectDatabaseField(Object object, TableMapInfo mapInfo)` | `Map<String, Object>` | 获取对象数据库字段值（仅含数据库映射字段） |
| `static fetchObjectFormResultSet(ResultSet rs, Class<T> clazz)` | `T` | 从 ResultSet 构建单个 Java 对象 |
| `static fetchObjectListFormResultSet(ResultSet rs, Class<T> clazz)` | `List<T>` | 从 ResultSet 构建对象列表 |

---

## 17. OrmDaoUtils (abstract class, 工具类, ~1128行)

ORM DAO 工具类。提供基于 JPA 注解实体的完整 CRUD 操作，包括级联操作、引用关系管理、批量操作。是 ORM 层最核心的 DAO 类。所有方法都需要传入 `Connection` 参数。

**路径**: `centit-database/src/main/java/com/centit/support/database/orm/OrmDaoUtils.java`

### 新增

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static saveNewObject(Connection conn, T object)` | `int` | 保存新对象（自动执行值生成器） |
| `static saveNewObjectAndFetchGeneratedKeys(Connection conn, T object)` | `Map<String, Object>` | 保存新对象并返回自增主键值 |

### 更新

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static updateObject(Connection conn, T object)` | `int` | 更新对象（所有字段） |
| `static updateObject(Connection conn, Collection<String> updateFields, T object)` | `int` | 更新对象的指定字段 |
| `static batchUpdateObject(Connection conn, Collection<String> updateFields, T object, Map<String, Object> properties)` | `int` | 批量更新（按条件更新指定字段） |
| `static mergeObject(Connection conn, T object)` | `int` | 合并对象（存在则更新，不存在则插入） |

### 查询 — 单对象

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static getObjectById(Connection conn, Object id, Class<T> clazz)` | `T` | 按主键查询 |
| `static getObjectByProperties(Connection conn, Map<String, Object> properties, Class<T> clazz)` | `T` | 按属性条件查询单个对象 |
| `static getObjectBySql(Connection conn, String sql, Map<String, Object> params, Class<T> clazz)` | `T` | 自定义 SQL 查询单个对象 |
| `static getObjectWithReferences(Connection conn, Object id, Class<T> clazz)` | `T` | 查询并加载所有引用关系（子表） |
| `static getObjectCascadeById(Connection conn, Object id, Class<T> clazz, int cascadeDepth)` | `T` | 级联查询（递归加载引用，指定深度） |

### 查询 — 列表

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static listAllObjects(Connection conn, Class<T> clazz)` | `List<T>` | 列出所有对象 |
| `static listObjectsByProperties(Connection conn, Map<String, Object> properties, Class<T> clazz)` | `List<T>` | 按属性条件列表查询 |
| `static listObjectsByProperties(Connection conn, Map<String, Object> properties, Class<T> clazz, int startPos, int maxSize)` | `List<T>` | 分页列表查询 |
| `static queryObjectsBySql(Connection conn, String sql, Class<T> clazz)` | `List<T>` | 自定义 SQL 列表查询 |
| `static countObjectByProperties(Connection conn, Map<String, Object> properties, Class<T> clazz)` | `int` | 按属性条件计数 |

### 删除

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static deleteObject(Connection conn, T object)` | `int` | 删除对象 |
| `static deleteObjectById(Connection conn, Object id, Class<T> clazz)` | `int` | 按主键删除 |
| `static deleteObjectByProperties(Connection conn, Map<String, Object> properties, Class<T> clazz)` | `int` | 按属性条件删除 |
| `static deleteObjectWithReferences(Connection conn, T object)` | `int` | 删除对象及其引用 |
| `static deleteObjectCascade(Connection conn, T object, int cascadeDepth)` | `int` | 级联删除（递归删除引用） |

### 引用关系操作

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static fetchObjectLazyColumn(Connection conn, T object, String columnName)` | `T` | 加载指定懒加载字段 |
| `static fetchObjectLazyColumns(Connection conn, T object)` | `T` | 加载所有懒加载字段 |
| `static fetchObjectReferences(Connection conn, T object)` | `T` | 加载所有引用关系 |
| `static fetchObjectReference(Connection conn, T object, String referenceName)` | `T` | 加载指定引用 |
| `static saveObjectReferences(Connection conn, T object)` | `int` | 保存所有引用关系 |
| `static saveNewObjectWithReferences(Connection conn, T object)` | `int` | 保存新对象及其引用 |
| `static saveNewObjectCascade(Connection conn, T object, int cascadeDepth)` | `int` | 级联保存（递归保存引用） |
| `static updateObjectCascade(Connection conn, T object, int cascadeDepth)` | `int` | 级联更新 |

### 其他

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static replaceObjectsAsTabulation(Connection conn, List<T> oldObjects, List<T> newObjects)` | `int` | 替换子表数据（智能对比增删改） |
| `static checkObjectExists(Connection conn, T object)` | `int` | 检查对象是否存在 |
| `static getSequenceNextValue(Connection conn, String sequenceName)` | `Long` | 获取序列下一个值 |

---

# 三、使用示例

### 示例 1：JPA 注解实体定义

```java
@Data
@Table(name = "F_USERINFO")
public class UserInfo implements Serializable {
    @Id
    @Column(name = "USER_CODE")
    @ValueGenerator(strategy = GeneratorType.UUID)
    private String userCode;

    @Column(name = "USER_NAME")
    private String userName;

    @Column(name = "LOGIN_DATE")
    @ValueGenerator(strategy = GeneratorType.FUNCTION, value = "sysdate", occasion = GeneratorTime.NEW_UPDATE)
    private Date lastUpdateDate;
}
```

### 示例 2：使用 OrmDaoUtils 操作实体

```java
// 保存新对象（自动生成 UUID 主键）
UserInfo user = new UserInfo();
user.setUserName("张三");
OrmDaoUtils.saveNewObject(conn, user);

// 按主键查询
UserInfo found = OrmDaoUtils.getObjectById(conn, "uuid-xxx", UserInfo.class);

// 按属性查询
Map<String, Object> props = new HashMap<>();
props.put("userName", "张三");
List<UserInfo> users = OrmDaoUtils.listObjectsByProperties(conn, props, UserInfo.class);

// 更新对象
found.setUserName("李四");
OrmDaoUtils.updateObject(conn, found);

// 删除
OrmDaoUtils.deleteObjectById(conn, "uuid-xxx", UserInfo.class);
```

### 示例 3：使用 DatabaseAccess 执行原生 SQL

```java
// 查询为 JSON
JSONArray result = DatabaseAccess.findObjectsAsJSON(conn,
    "SELECT * FROM users WHERE age > ?", new Object[]{18},
    new String[]{"USER_CODE", "USER_NAME"});

// 分页查询
JSONArray paged = DatabaseAccess.findObjectsAsJSON(conn,
    "SELECT * FROM users", null, null, 2, 10);  // 第2页，每页10条

// 执行更新
int affected = DatabaseAccess.doExecuteSql(conn,
    "UPDATE users SET status = ? WHERE age > ?", new Object[]{"active", 18});
```

### 示例 4：使用 QueryUtils 构建分页 SQL

```java
String sql = "SELECT u.USER_CODE, u.USER_NAME FROM F_USERINFO u WHERE u.STATUS = 'active' ORDER BY u.USER_NAME";

// 构建 Oracle 分页 SQL
String pagedSql = QueryUtils.buildLimitQuerySQL(sql, 0, 20, true, DBType.Oracle);
// 结果: SELECT * FROM (SELECT t.*, ROWNUM rn FROM (原SQL) t WHERE ROWNUM <= 20) WHERE rn > 0

// 构建计数 SQL
String countSql = QueryUtils.buildGetCountSQL(sql);
// 结果: SELECT count(*) FROM F_USERINFO u WHERE u.STATUS = 'active'
```
