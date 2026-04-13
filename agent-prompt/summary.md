# centit-persistence 项目总体摘要

> 本文件供 AI Agent 分步加载模块文档时使用。建议先阅读本摘要，再按需加载具体模块文件。

## 项目概述

centit-persistence 是一个数据库持久化框架，提供跨多种数据库的统一 JDBC 操作、ORM 映射（封装了 JPA 注解 + Spring JDBC）、DDL 操作、元数据读取等功能。框架不依赖 Hibernate/JPA 运行时，仅使用 JPA 注解作为元数据标记。

**支持的数据库**：Oracle、MySQL、PostgreSQL、DB2、SQL Server、H2、SQLite、ClickHouse、达梦(DM)、人大金仓(KingBase)、南大通用(GBase)、神通(Oscar)

## 模块依赖关系

```
centit-persistence-jdbc (Spring JDBC ORM 框架，顶层入口)
    ├── centit-database (核心数据库抽象层)
    │   ├── utils 包    — DBType, FieldType, QueryUtils, DatabaseAccess, PageDesc
    │   ├── orm 包      — JpaMetadata, OrmUtils, OrmDaoUtils, ValueGenerator
    │   ├── metadata 包 — TableInfo/TableField/TableReference, DatabaseMetadata, PdmReader
    │   ├── jsonmaptable 包 — JsonObjectDao, GeneralJsonObjectDao
    │   └── ddl 包      — DDLOperations, GeneralDDLOperations
    │
    ├── centit-database-datasource (HikariCP 连接池)
    │   └── centit-database
    │
    ├── centit-database-transaction (JDBC 事务管理)
    │   └── centit-database-datasource
    │
    └── centit-database-dynamic-datasource (Spring AOP 动态数据源切换)
        └── centit-database (仅使用工具类)
```

## 模块文档索引

| 文件 | 涵盖模块/功能 | 典型使用场景 |
|------|--------------|-------------|
| [centit-database-core.md](centit-database-core.md) | centit-database 的 utils + orm 包 | 编写 DAO/Service、构建 SQL 查询、ORM 映射配置 |
| [centit-database-metadata-ddl.md](centit-database-metadata-ddl.md) | centit-database 的 metadata + jsonmaptable + ddl 包 | DDL 建表/改表、元数据读取、动态表操作(Map/JSON DAO) |
| [centit-database-datasource.md](centit-database-datasource.md) | 连接池管理 | 数据源配置、连接获取、编程式事务 |
| [centit-database-dynamic-datasource.md](centit-database-dynamic-datasource.md) | 动态数据源切换 | 多数据源场景、分库路由 |
| [centit-database-transaction.md](centit-database-transaction.md) | JDBC 事务管理 | 声明式事务(@JdbcTransaction)、ThreadLocal 连接管理 |
| [centit-persistence-jdbc.md](centit-persistence-jdbc.md) | Spring JDBC ORM 框架 | 编写实体 DAO、查询构建、分页、逻辑删除、版本控制 |

## 核心包名映射

| 包名 | 所属模块文档 |
|------|-------------|
| `com.centit.support.database.utils` | centit-database-core.md |
| `com.centit.support.database.orm` | centit-database-core.md |
| `com.centit.support.database.metadata` | centit-database-metadata-ddl.md |
| `com.centit.support.database.jsonmaptable` | centit-database-metadata-ddl.md |
| `com.centit.support.database.ddl` | centit-database-metadata-ddl.md |
| `com.centit.support.database.utils` (DataSourceDescription 等) | centit-database-datasource.md |
| `com.centit.support.database.transaction` | centit-database-transaction.md |
| `com.centit.framework.core.datasource` | centit-database-dynamic-datasource.md |
| `com.centit.framework.jdbc.dao` | centit-persistence-jdbc.md |
| `com.centit.framework.core.dao` | centit-persistence-jdbc.md |
| `com.centit.framework.core.po` | centit-persistence-jdbc.md |

## 快速场景指南

### 场景 1：编写实体 DAO
加载：`centit-database-core.md` + `centit-persistence-jdbc.md`
关键类：`BaseDaoImpl`, `OrmDaoUtils`, `JpaMetadata`, `ValueGenerator`

### 场景 2：构建 SQL 查询（不使用 ORM）
加载：`centit-database-core.md`
关键类：`QueryUtils`, `DatabaseAccess`, `PageDesc`

### 场景 3：配置数据源和连接池
加载：`centit-database-datasource.md`
关键类：`DataSourceDescription`, `DbcpConnectPools`

### 场景 4：多数据源动态切换
加载：`centit-database-dynamic-datasource.md`
关键类：`@TargetDataSource`, `DynamicDataSource`, `DynamicDataSourceContextHolder`

### 场景 5：JDBC 事务管理
加载：`centit-database-transaction.md`
关键类：`@JdbcTransaction`, `ConnectThreadHolder`, `DatabaseWorkHandler`

### 场景 6：DDL 建表/改表
加载：`centit-database-metadata-ddl.md`
关键类：`DDLOperations`, `GeneralDDLOperations`, `TableInfo`

### 场景 7：动态表操作（无 PO 实体）
加载：`centit-database-metadata-ddl.md` + `centit-persistence-jdbc.md`(JsonObjectWork 部分)
关键类：`JsonObjectDao`, `GeneralJsonObjectDao`, `JsonObjectWork`

### 场景 8：数据库元数据读取
加载：`centit-database-metadata-ddl.md`
关键类：`DatabaseMetadata`, `JdbcMetadata`, `PdmReader`

## Maven 坐标

```xml
<dependency>
    <groupId>com.centit.support</groupId>
    <artifactId>centit-database</artifactId>
</dependency>
<dependency>
    <groupId>com.centit.support</groupId>
    <artifactId>centit-database-datasource</artifactId>
</dependency>
<dependency>
    <groupId>com.centit.support</groupId>
    <artifactId>centit-database-transaction</artifactId>
</dependency>
<dependency>
    <groupId>com.centit.framework</groupId>
    <artifactId>centit-database-dynamic-datasource</artifactId>
</dependency>
<dependency>
    <groupId>com.centit.framework</groupId>
    <artifactId>centit-persistence-jdbc</artifactId>
</dependency>
```

版本号由父 POM 统一管理，当前为 `JDK21-SNAPSHOT`。
