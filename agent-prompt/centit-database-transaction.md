# centit-database-transaction 模块

> Maven: `com.centit.support:centit-database-transaction`
> 包名: `com.centit.support.database.transaction`
> 核心依赖: centit-database-datasource + spring-context(optional) + spring-aspects(optional)

## 模块概述

JDBC 连接事务管理模块，通过 ThreadLocal 保存数据库连接。提供两种事务管理方式：
1. **声明式**：`@JdbcTransaction` 注解 + Spring AOP 切面自动提交/回滚
2. **编程式**：`ConnectThreadHolder` 静态方法手动管理，或 `DatabaseWorkHandler` 模板方法

支持多数据源事务：同一线程可同时操作多个数据源，各数据源事务独立管理。

---

## 架构层次

```
┌─────────────────────────────────────────────────────┐
│  @JdbcTransaction 注解方法 (声明式事务)               │
│  或 DatabaseWorkHandler.executeInTransaction (编程式)  │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────┐
│  JdbcTransactionAspect (AOP 切面, @Aspect @Component) │
│  @AfterReturning  → commitAndRelease()               │
│  @AfterThrowing   → rollbackAndRelease()             │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────┐
│  ConnectThreadHolder (全局静态入口)                    │
│  fetchConnect() | commitAndRelease() | rollback...    │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────┐
│  ConnectThreadLocal extends ThreadLocal               │
│  remove() → 自动回滚+释放; superRemove() → 仅清除引用   │
└──────────────────┬──────────────────────────────────┘
                   ▼
┌─────────────────────────────────────────────────────┐
│  ConnectThreadWrapper (线程级连接容器)                  │
│  ConcurrentHashMap<DataSourceDescription, Connection> │
│  fetchConnect() | commitAllWork() | rollbackAllWork() │
└─────────────────────────────────────────────────────┘
```

---

## 一、@JdbcTransaction (注解)

标记方法需要 JDBC 事务管理。无属性的标记型注解。

**路径**: `centit-database-transaction/src/main/java/com/centit/support/database/transaction/JdbcTransaction.java`

**元注解**: `@Target(ElementType.METHOD)`, `@Retention(RetentionPolicy.RUNTIME)`

**事务策略**：
- 方法正常返回 → 自动提交
- 方法抛出 `RuntimeException` → 自动回滚
- 非 RuntimeException（checked Exception）**不会触发回滚**

---

## 二、JdbcTransactionAspect (class, @Aspect @Component)

Spring AOP 切面，拦截 `@JdbcTransaction` 方法，自动管理事务提交/回滚。

**路径**: `centit-database-transaction/src/main/java/com/centit/support/database/transaction/JdbcTransactionAspect.java`

### 方法

| 方法签名 | 返回类型 | 注解 | 说明 |
|---------|---------|------|------|
| `transactionAspect()` | `void` | `@Pointcut("@annotation(..JdbcTransaction)")` | 切入点定义 |
| `doAfterThrowing(JoinPoint, JdbcTransaction, Throwable)` | `void` | `@AfterThrowing` | 异常通知：若为 RuntimeException 则 `ConnectThreadHolder.rollbackAndRelease()` |
| `doAfterReturning(JoinPoint, JdbcTransaction)` | `void` | `@AfterReturning` | 返回通知：`ConnectThreadHolder.commitAndRelease()` |

---

## 三、ConnectThreadHolder (class, 工具类)

全局静态入口，提供线程安全的连接获取、事务提交/回滚操作。整个模块对外的**核心 API**。

**路径**: `centit-database-transaction/src/main/java/com/centit/support/database/transaction/ConnectThreadHolder.java`

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static getConnectThreadWrapper()` | `ConnectThreadWrapper` | 获取当前线程的连接包装器（惰性初始化） |
| `static fetchConnect(DataSourceDescription description)` | `Connection` | 获取指定数据源的 JDBC 连接（同线程同数据源复用） |
| `static fetchConnect(IDatabaseInfo description)` | `Connection` | 通过 IDatabaseInfo 获取连接（自动转换为 DataSourceDescription） |
| `static commitAndRelease()` | `void` | 提交所有事务 + 释放所有连接 + 清除 ThreadLocal |
| `static rollbackAndRelease()` | `void` | 回滚所有事务 + 释放所有连接 + 清除 ThreadLocal |

---

## 四、ConnectThreadLocal (class)

自定义 ThreadLocal 实现，在 `remove()` 时自动回滚并释放所有连接，防止连接泄漏。

**路径**: `centit-database-transaction/src/main/java/com/centit/support/database/transaction/ConnectThreadLocal.java`

**继承**: `ThreadLocal<ConnectThreadWrapper>`

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `remove()` | `void` | 重写 ThreadLocal.remove()。先回滚所有事务 + 释放所有连接，再清除 ThreadLocal |
| `superRemove()` | `void` | 仅调用 super.remove()，跳过回滚/释放（用于已手动管理后清理引用） |

---

## 五、ConnectThreadWrapper (class, Serializable)

线程级别的数据库连接容器，管理当前线程持有的多个数据源连接。

**路径**: `centit-database-transaction/src/main/java/com/centit/support/database/transaction/ConnectThreadWrapper.java`

### 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `connectPools` | `Map<DataSourceDescription, Connection>` | 数据源→连接映射（ConcurrentHashMap，初始容量4） |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `fetchConnect(DataSourceDescription description)` | `Connection` | 获取指定数据源连接。已有则复用，否则从 DbcpConnectPools 获取新连接 |
| `commitAllWork()` | `void` | 提交所有连接上的事务 |
| `rollbackAllWork()` | `void` | 回滚所有连接上的事务 |
| `releaseAllConnect()` | `void` | 归还所有连接到连接池并清空映射 |

---

## 六、DatabaseWorkHandler (abstract class, 工具类)

提供在事务中执行数据库操作的模板方法。支持函数式接口（lambda）。

**路径**: `centit-database-transaction/src/main/java/com/centit/support/database/transaction/DatabaseWorkHandler.java`

### 内部函数式接口

| 接口名 | 方法签名 | 说明 |
|--------|---------|------|
| `ExecuteWork<T>` | `T execute(Connection conn) throws SQLException` | 写操作工作单元 |
| `QueryWork<T>` | `T execute(Connection conn) throws SQLException, IOException` | 读操作工作单元（可抛 IOException） |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static executeInTransaction(DataSourceDescription dataSourceDesc, ExecuteWork<T> realWork)` | `T` | 从 ConnectThreadHolder 获取连接，执行写操作。**注意：此方法本身不管理事务，依赖 AOP 或手动管理** |
| `static executeQueryInTransaction(DataSourceDescription dataSourceDesc, QueryWork<T> realWork)` | `T` | 从 ConnectThreadHolder 获取连接，执行读操作 |

---

## 使用示例

### 示例 1：声明式事务（推荐）

```java
@Service
public class OrderService {

    @JdbcTransaction
    public void createOrder(Order order, List<OrderItem> items) {
        // 所有操作共享同一连接
        Connection conn = ConnectThreadHolder.fetchConnect(dataSourceDesc);
        
        OrmDaoUtils.saveNewObject(conn, order);
        for (OrderItem item : items) {
            item.setOrderId(order.getOrderId());
            OrmDaoUtils.saveNewObject(conn, item);
        }
        // 方法正常返回 → 自动 commit
        // 抛出 RuntimeException → 自动 rollback
    }
}
```

### 示例 2：编程式事务（手动管理）

```java
// 写操作
DatabaseWorkHandler.executeInTransaction(dsDesc, conn -> {
    OrmDaoUtils.saveNewObject(conn, user);
    return null;
});
ConnectThreadHolder.commitAndRelease();

// 读操作
Connection conn = ConnectThreadHolder.fetchConnect(dsDesc);
List<User> users = OrmDaoUtils.listAllObjects(conn, User.class);
ConnectThreadHolder.commitAndRelease();
```

### 示例 3：多数据源事务

```java
@JdbcTransaction
public void syncData() {
    // 从源数据库读取
    Connection sourceConn = ConnectThreadHolder.fetchConnect(sourceDsDesc);
    List<Object[]> data = DatabaseAccess.findObjectsBySql(sourceConn, "SELECT * FROM users", null);
    
    // 写入目标数据库
    Connection targetConn = ConnectThreadHolder.fetchConnect(targetDsDesc);
    for (Object[] row : data) {
        DatabaseAccess.doExecuteSql(targetConn, "INSERT INTO users_copy VALUES(?,?,?)", row);
    }
    // commitAndRelease() 会提交所有连接上的事务
}
```

## 设计说明

1. **ThreadLocal 连接复用**：同一线程对同一数据源始终使用同一连接，确保事务原子性
2. **多数据源支持**：`ConnectThreadWrapper` 内部使用 Map 管理多个连接
3. **防御性清理**：`ConnectThreadLocal.remove()` 在清除前自动回滚+释放，防止连接泄漏
4. **注意**：`DatabaseWorkHandler.executeInTransaction()` 本身不管理事务生命周期，需配合 `@JdbcTransaction` 或手动调用 `commitAndRelease()`
