# centit-database-dynamic-datasource 模块

> Maven: `com.centit.framework:centit-database-dynamic-datasource`
> 包名: `com.centit.framework.core.datasource`
> 核心依赖: centit-database + spring-jdbc(provided) + spring-aspects

## 模块概述

Spring AOP 动态数据源切换模块。通过 `@TargetDataSource` 注解 + ThreadLocal + `AbstractRoutingDataSource` 实现运行时多数据源动态路由。支持固定数据源名称和基于方法参数的动态表达式计算。

---

## 架构流程

```
调用方 (Service/DAO)
    │ 调用被 @TargetDataSource("dsX") 标注的方法
    ▼
DynamicDataSourceAspect.doBefore()
    ├── 解析注解值
    ├── 若 mapByParameter=true: VariableFormula.calculate(表达式, 方法参数) → 动态数据源名
    ├── 若 mapByParameter=false: 直接使用注解值
    └── DynamicDataSourceContextHolder.setDataSourceType("dsX")
    ▼
目标方法执行 → 需要数据库连接
    ▼
DynamicDataSource.determineCurrentLookupKey()
    └── 返回 ThreadLocal 中的 "dsX" → AbstractRoutingDataSource 路由到对应数据源
    ▼
方法返回/异常 → DynamicDataSourceAspect.doAfter*/doAfterThrowing
    └── DynamicDataSourceContextHolder.clearDataSourceType()  // 恢复默认数据源
```

---

## 一、@TargetDataSource (注解)

声明式标注目标方法/类应使用的数据源。运行时保留，供 AOP 切面拦截。

**路径**: `centit-database-dynamic-datasource/src/main/java/com/centit/framework/core/datasource/TargetDataSource.java`

**元注解**: `@Target({METHOD, TYPE})`, `@Retention(RUNTIME)`, `@Documented`

### 注解属性

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `value()` | `String` | `""` | 数据源名称。与 `name` 互为别名（`@AliasFor`） |
| `name()` | `String` | `""` | 数据源名称。与 `value` 互为别名（`@AliasFor`） |
| `mapByParameter()` | `boolean` | `false` | 是否将 value 当作表达式，根据方法参数动态计算数据源名称 |

---

## 二、DynamicDataSourceContextHolder (class)

基于 ThreadLocal 维护当前线程的数据源标识。是动态数据源机制的**核心状态容器**。

**路径**: `centit-database-dynamic-datasource/src/main/java/com/centit/framework/core/datasource/DynamicDataSourceContextHolder.java`

### 字段

| 修饰符 | 类型 | 名称 | 说明 |
|--------|------|------|------|
| `private static final` | `ThreadLocal<String>` | `contextHolder` | 线程本地变量，存储当前数据源名称 |

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `static setDataSourceType(String dataSourceType)` | `void` | 设置当前线程的数据源类型（写入 ThreadLocal） |
| `static getDataSourceType()` | `String` | 获取当前线程的数据源类型（读取 ThreadLocal） |
| `static clearDataSourceType()` | `void` | 清除当前线程的数据源类型（移除 ThreadLocal），恢复为默认数据源 |

---

## 三、DynamicDataSourceAspect (class, @Aspect)

Spring AOP 切面，拦截 `@TargetDataSource` 方法，在方法前切换数据源，在方法后（正常或异常）恢复默认。

**路径**: `centit-database-dynamic-datasource/src/main/java/com/centit/framework/core/datasource/DynamicDataSourceAspect.java`

### 外部依赖

- `VariableFormula`（来自 centit-support-compiler）— 表达式引擎，动态计算数据源名称
- `ParamName`（来自 centit-support-common）— 自定义参数名注解，解决编译时参数名丢失

### 方法

| 方法签名 | 返回类型 | 注解 | 说明 |
|---------|---------|------|------|
| `electDataSourceAspect()` | `void` | `@Pointcut("@annotation(..TargetDataSource)")` | 切入点：所有 @TargetDataSource 方法 |
| `static getMethodDescription(JoinPoint)` | `Map<String, Object>` | — | 提取方法参数名→值映射，优先用 @ParamName |
| `doBefore(JoinPoint, TargetDataSource)` | `void` | `@Before` | 前置通知：解析注解，切换数据源 |
| `doAfterThrowing(JoinPoint, TargetDataSource, Throwable)` | `void` | `@AfterThrowing` | 异常通知：清除 ThreadLocal |
| `doAfterReturning(JoinPoint, TargetDataSource)` | `void` | `@AfterReturning` | 返回通知：清除 ThreadLocal |

### doBefore 内部逻辑

1. 从注解获取 `value()`
2. 若 `value` 非空：
   - `mapByParameter == true`：调用 `getMethodDescription()` 获取参数映射 → `VariableFormula.calculate()` 计算表达式 → 实际数据源名
   - `mapByParameter == false`：直接使用 `value` 作为数据源名
3. 调用 `DynamicDataSourceContextHolder.setDataSourceType()` 写入 ThreadLocal

---

## 四、DynamicDataSource (class)

Spring `AbstractRoutingDataSource` 的具体实现，根据 ThreadLocal 中的 key 动态路由数据源。

**路径**: `centit-database-dynamic-datasource/src/main/java/com/centit/framework/core/datasource/DynamicDataSource.java`

**继承**: `org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource`

### 方法

| 方法签名 | 返回类型 | 说明 |
|---------|---------|------|
| `protected determineCurrentLookupKey()` | `Object` | 重写抽象方法。返回 `DynamicDataSourceContextHolder.getDataSourceType()`，为 null 时使用默认数据源 |

---

## 使用示例

### 示例 1：固定数据源切换

```java
@Service
public class ReportService {

    @TargetDataSource("reportDb")
    public List<Map<String, Object>> getReportData() {
        // 此方法将使用名为 "reportDb" 的数据源
        return jdbcTemplate.queryForList("SELECT * FROM report_data");
    }

    // 无注解，使用默认数据源
    public void saveLog(String message) {
        jdbcTemplate.update("INSERT INTO logs(message) VALUES(?)", message);
    }
}
```

### 示例 2：动态表达式路由（分库场景）

```java
@Service
public class OrderService {

    @TargetDataSource(value = "'ds' + (userId % 4 + 1)", mapByParameter = true)
    public Order getOrderByUser(@ParamName("userId") long userId, String orderId) {
        // 根据 userId 动态选择数据源: ds1, ds2, ds3, ds4
        return orderDao.findById(orderId);
    }
}
```

### 示例 3：Spring 配置

```java
@Configuration
public class DataSourceConfig {

    @Bean
    @Primary
    public DynamicDataSource dynamicDataSource(
            @Qualifier("masterDataSource") DataSource master,
            @Qualifier("reportDataSource") DataSource report) {
        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put("master", master);
        targetDataSources.put("reportDb", report);

        DynamicDataSource dynamicDataSource = new DynamicDataSource();
        dynamicDataSource.setDefaultTargetDataSource(master);
        dynamicDataSource.setTargetDataSources(targetDataSources);
        return dynamicDataSource;
    }
}
```

## 设计说明

1. **线程安全**：ThreadLocal 保证每个线程独立的数据源上下文
2. **自动清理**：`@Before` + `@AfterReturning` + `@AfterThrowing` 确保无论方法是否异常，ThreadLocal 都会被清理
3. **动态表达式**：通过 `VariableFormula` 引擎支持基于方法参数的动态数据源选择
4. **Spring 标准集成**：继承 `AbstractRoutingDataSource`，兼容 XML/Java Config
