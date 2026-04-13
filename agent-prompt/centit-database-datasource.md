# centit-database-datasource 模块

> Maven: `com.centit.support:centit-database-datasource`
> 包名: `com.centit.support.database.utils`
> 核心依赖: centit-database + HikariCP

## 模块概述

基于 HikariCP 的数据库连接池管理模块。提供数据源描述配置、连接池创建与缓存、编程式事务模板方法。不依赖 Spring 框架，纯 JDBC 实现。

---

## 一、DataSourceDescription (final class)

数据源描述信息 POJO，封装创建连接池所需的全部参数。

**路径**: `centit-database-datasource/src/main/java/com/centit/support/database/utils/DataSourceDescription.java`

### 构造方法

| 签名 | 说明 |
|------|------|
| `DataSourceDescription()` | 默认构造，连接池参数使用默认值 |
| `DataSourceDescription(String connUrl, String username)` | 指定 URL 和用户名 |
| `DataSourceDescription(String connUrl, String username, String password)` | 指定 URL、用户名和密码 |

### 字段（含 getter/setter）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `connUrl` | `String` | null | JDBC 连接 URL。**setConnUrl() 会自动推导 dbType 和 driver** |
| `username` | `String` | null | 数据库用户名 |
| `password` | `String` | null | 数据库密码 |
| `driver` | `String` | null | JDBC 驱动类名 |
| `dbType` | `DBType` | null | 数据库类型枚举（由 connUrl 自动推导，只读） |
| `maxTotal` | `int` | 20 | 最大活动连接数 |
| `maxIdle` | `int` | 5 | 最大空闲连接数 |
| `minIdle` | `int` | 2 | 最小空闲连接数 |
| `maxWaitMillis` | `int` | 10000 | 最大等待时间(毫秒) |
| `initialSize` | `int` | 5 | 初始连接数 |
| `databaseCode` | `String` | null | 数据库编码标识 |

### 关键方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `testConnect()` | `boolean` | 测试当前数据源描述是否能成功建立连接 |
| `static testConnect(DataSourceDescription dsDesc)` | `boolean` | 静态方法，测试指定数据源描述是否能连接 |
| `static valueOf(IDatabaseInfo dbinfo)` | `DataSourceDescription` | 从 IDatabaseInfo 接口转换，读取扩展属性中的连接池参数 |
| `equals(Object)` | `boolean` | 基于 connUrl + username 判等 |
| `hashCode()` | `int` | 基于 connUrl + username 计算 |

---

## 二、DbcpConnectPools (abstract class, 工具类)

基于 HikariCP 的连接池管理器。维护全局 `ConcurrentHashMap<DataSourceDescription, HikariDataSource>` 缓存，相同 URL+username 共享连接池实例。

**路径**: `centit-database-datasource/src/main/java/com/centit/support/database/utils/DbcpConnectPools.java`

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static synchronized getDataSource(DataSourceDescription dsDesc)` | `HikariDataSource` | 获取或创建对应数据源的 HikariDataSource 连接池 |
| `static synchronized getDbcpConnect(DataSourceDescription dsDesc)` | `Connection` | 从连接池获取连接，设置 autoCommit=false |
| `static getDataSource(IDatabaseInfo dbinfo)` | `HikariDataSource` | 通过 IDatabaseInfo 获取连接池 |
| `static getDbcpConnect(IDatabaseInfo dbinfo)` | `Connection` | 通过 IDatabaseInfo 获取连接 |
| `static getDataSourceStats(DataSourceDescription dsDesc)` | `Map<String, Object>` | 获取连接池状态信息 |
| `static synchronized shutdownDataSource()` | `void` | 关闭所有连接池 |
| `static synchronized testDataSource(DataSourceDescription dsDesc)` | `boolean` | 测试数据源连接（创建临时池验证后关闭） |
| `static closeConnect(Connection conn)` | `void` | 安全关闭连接（捕获异常仅记录日志） |

### HikariCP 配置参数

内部 `mapDataSource()` 方法将 DataSourceDescription 映射为 HikariDataSource 时的关键参数：

| 参数 | 值 | 说明 |
|------|-----|------|
| `connectionTimeout` | 30000 (30s) | 连接超时 |
| `maximumPoolSize` | dsDesc.maxTotal | 最大池大小 |
| `maxLifetime` | 1800000 (30min) | 连接最大生命周期 |
| `idleTimeout` | 60000 (60s) | 空闲超时 |
| `validationTimeout` | 5000 (5s) | 验证超时 |
| `minimumIdle` | dsDesc.minIdle | 最小空闲 |
| `connectionTestQuery` | 按 DBType 自动选择 | 连接验证查询 |

---

## 三、TransactionHandler (abstract class, 工具类)

编程式事务处理器，提供模板方法模式封装事务生命周期（获取连接 → 执行 → 提交/回滚 → 关闭）。设计为函数式接口，支持 Java 8 lambda。

**路径**: `centit-database-datasource/src/main/java/com/centit/support/database/utils/TransactionHandler.java`

### 内部函数式接口

| 接口名 | 方法签名 | 说明 |
|--------|---------|------|
| `TransactionWork<T>` | `T execute(Connection conn) throws SQLException` | 事务性工作单元（写操作） |
| `QueryWork<T>` | `T execute(Connection conn) throws SQLException, IOException` | 查询工作单元（读操作，可抛 IOException） |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static executeInTransaction(DataSourceDescription dataSourceDesc, TransactionWork<T> realWork)` | `T` | 从数据源获取连接，执行事务工作，成功 commit，异常 rollback |
| `static executeInTransaction(Connection conn, TransactionWork<T> realWork)` | `T` | 在已有连接上执行事务，成功 commit，异常 rollback |
| `static executeQueryInTransaction(DataSourceDescription dataSourceDesc, QueryWork<T> realWork)` | `T` | 从数据源获取连接，执行查询，仅异常处理 |
| `static executeQueryInTransaction(Connection conn, QueryWork<T> realWork)` | `T` | 在已有连接上执行查询，仅异常处理 |

### 事务行为

- `executeInTransaction`: 成功 → `conn.commit()`；异常 → `conn.rollback()` + 重新抛出 SQLException
- `executeQueryInTransaction`: 仅捕获异常并重新抛出，不执行 commit/rollback

---

## 使用示例

### 示例 1：配置数据源并获取连接

```java
DataSourceDescription dsDesc = new DataSourceDescription(
    "jdbc:oracle:thin:@localhost:1521:orcl", "username", "password");
Connection conn = DbcpConnectPools.getDbcpConnect(dsDesc);
// ... 使用 conn 执行操作 ...
DbcpConnectPools.closeConnect(conn);
```

### 示例 2：编程式事务

```java
DataSourceDescription dsDesc = new DataSourceDescription("jdbc:mysql://localhost:3306/test", "root", "123456");

// 写事务
TransactionHandler.executeInTransaction(dsDesc, conn -> {
    DatabaseAccess.doExecuteSql(conn, "INSERT INTO users(name) VALUES(?)", new Object[]{"test"});
    return null;
});

// 读查询
List<Object[]> result = TransactionHandler.executeQueryInTransaction(dsDesc, conn -> {
    return DatabaseAccess.findObjectsBySql(conn, "SELECT * FROM users", null);
});
```

### 示例 3：从 IDatabaseInfo 创建

```java
// IDatabaseInfo 通常由框架的数据库配置服务提供
DataSourceDescription dsDesc = DataSourceDescription.valueOf(databaseInfo);
Connection conn = DbcpConnectPools.getDbcpConnect(dsDesc);
```

### 示例 4：ORM 操作 + 事务

```java
DataSourceDescription dsDesc = new DataSourceDescription(connUrl, username, password);

TransactionHandler.executeInTransaction(dsDesc, conn -> {
    UserInfo user = new UserInfo();
    user.setUserName("张三");
    OrmDaoUtils.saveNewObject(conn, user);
    
    List<UserInfo> allUsers = OrmDaoUtils.listAllObjects(conn, UserInfo.class);
    System.out.println(JSONArray.toJSONString(allUsers));
    return null;
});
```
