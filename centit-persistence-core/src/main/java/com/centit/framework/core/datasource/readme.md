# 1. Spring-DynamicDataSource 是什么？
**DynamicDataSource** 动态数据源，为 Spring 提供动态数据源获取支持。

> **原理：**
> 
> **DynamicDataSource** 采用 AOP 机制拦截所有使用了注解 `@TargetDataSource("...")` 定义的类和方法，然后在此类中的方法和这些方法**被调用之前自动切换**当前数据源为 `@TargetDataSource("...")` 定义的数据源，为方法中使用提供适合的数据源（`org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource` 提供了多数据源注册和根据注册类型查找对应的数据源能力）。


# 2. DynamicDataSource 如何使用？
**DynamicDataSource** 使用很简单：

步骤1： 在Spring配置文件中配置多个数据源。
```xml
<bean id="ds1" class="com.alibaba.druid.pool.DruidDataSource" ...>...</bean>
<bean id="ds2" class="com.alibaba.druid.pool.DruidDataSource" ...>...</bean>
<bean id="ds3" class="com.alibaba.druid.pool.DruidDataSource" ...>...</bean>
<bean id="ds4" class="com.alibaba.druid.pool.DruidDataSource" ...>...</bean>
```

步骤2： 将这些数据源注册给 `com.gdk.spring.datasource.DynamicDataSource`。
```xml
<bean id="dynamic_datasource" class="com.gdk.spring.datasource.DynamicDataSource">
    <!-- 注册目标多个数据源 -->
	<property name="targetDataSources">  
		<map key-type="java.lang.String">
		   <!--这里的key值将是 @TargetDataSource("...") 的值-->  
		   <entry key="ds1" value-ref="ds1"/> 
		   <entry key="ds2" value-ref="ds2"/>
		   <entry key="ds2" value-ref="ds3"/>
		   <entry key="ds2" value-ref="ds4"/>
		</map>  
    </property>
    <!-- 如果出现了未知的数据源，则将使用这里配置的默认数据源替代 -->
    <property name="defaultTargetDataSource" ref="ds1"/>  
</bean>
```

步骤3： 配置生效 `DynamicDataSourceAspect` AOP切面拦截器。
```xml
<bean id="dynamicDataSourceAspect" class="com.gdk.spring.datasource.DynamicDataSourceAspect">
	<!-- 可选，自定义数据源选举策略，默认 SimpleDataSourceTypeElectPolicy
	<property name="dataSourceTypeElectPolicy">
		<bean class="com.gdk.spring.datasource.SimpleDataSourceTypeElectPolicy" />
	</property>
	-->
</bean>
```

步骤4： 使用注解 `@TargetDatasource("...")` 定义类或方法要用到的数据源即可。
```java
/** 
 * use on class, all methods in this class will be use 'ds1' datasource.
 */
@Service("userService")
@TargetDataSource("ds1")
public class UserService {
    public List<User> findUsers() {
        // ...
    }
}
```

```java
/** 
 * use on method, the method will be use 'ds4' datasource.
 */
@Service("orderService")
public class OrderService {
    
    @TargetDataSource("ds4")
    public List<OrderItem> findOrderItems() {
        // ...
    }
}
```

```java
/** 
 * use on class and some method, 
 * the unannotationed method will be use 'ds2' datasource,
 * and the annotationed method will be use 'ds3' datasource.
 */
@Service("shoppingCartService")
@TargetDataSource("ds2")
public class ShoppingCartService {

    public List<ShoppingItem> findShoppingItems() {
        // ...
    }    

    @TargetDataSource("ds3")
    public Address getDefaultDeliverAddress() {
        // ...
    }
}

```
步骤5[可选]：以集成 MyBatis 为例，设置 MyBatis 的 `SqlSessionFactoryBean` 数据源 和 事务管理器数据源。
```xml
<bean id="sqlSessionFactory" class="org.mybatis.spring.SqlSessionFactoryBean">
  <property name="dataSource" ref="dynamic_datasource" />
  <property name="configLocation" value="..." />
  <property name="mapperLocations" value="..." />
</bean>

<bean id="transactionManager" class="org.springframework.jdbc.datasource.DataSourceTransactionManager">
  <property name="dataSource" ref="dynamic_datasource" />
</bean>
```


# 3. 定制特殊的DataSource类型选举策略（DataSourceTypeElectPolicy）
**DynamicDataSource** 默认的数据源选举策略就是直接使用 `@TargetDataSource("...")` 设置的数据源，这种选举策略可以满足绝大部分动态数据源要求。

**但是，想象下下面的这种数据源场景：**
> 由于用户信息庞大，为了避免单表海量数据，因此决定对用户信息进行分库存储，根据用户ID hash（id % 10） 到10个库中。
>
> 现在要根据用户ID获取此用户的信息，此时，动态数据源如何配置？

假设10个用户分库配置信息如下：
```xml
<bean id="user01" class="com.alibaba.druid.pool.DruidDataSource" ...>...</bean>
<bean id="user02" class="com.alibaba.druid.pool.DruidDataSource" ...>...</bean>
<bean id="..." class="com.alibaba.druid.pool.DruidDataSource" ...>...</bean>
<bean id="user10" class="com.alibaba.druid.pool.DruidDataSource" ..>...</bean>

<bean id="dynamic_datasource" class="com.gdk.spring.datasource.DynamicDataSource">
	<property name="targetDataSources">  
		<map key-type="java.lang.String">
		   <entry key="user01" value-ref="user01"/> 
		   <entry key="user02" value-ref="user02"/>
		   <entry key="..." value-ref="..."/>
		   <entry key="user10" value-ref="user10"/>
		</map>  
    </property>
    <property name="defaultTargetDataSource" ref="user01"/>  
</bean>
```
用户业务处理类 `UserService` 如下：
```java
@Service("userService")
public class UserService {
    
    // how to change datasource in this method ?
    public User getUserById(long userId) {
        return ...;
    }
}
```
那么，当调用 `userService.getUserById(long userId)` 该如何分配相应的数据源呢？

肯定不可能为每个用户ID都分配数据源，这时，如果能够支持根据方法传入的**用户ID参数值**按照分库规则动态计算出相应的数据源就好了。

此时，就是 **定制DataSource类型选举策略** 作用的时候了，看：
```java
package com.my;
public class MyDataSourceTypeElectPolicy implements DataSourceTypeElectPolicy {
    @Override
    public Object electDataSourceType(TargetDataSource targetDataSource, JoinPoint joinPoint) {
        Object dsType = targetDataSource.value();
        Object[] args = joinPoint.getArgs();
        
        // if exists other datasource used
        if (!"user".equals(String.valueOf(dsType))) {
            return dsType;
        }
        
        if (args == null || args.length == 0) {
            return dsType;
        }
        
        // example suppose
        Object param = args[0]; // userId?
        if (param != null && ((param instanceof Integer) || (param instanceof Long))) {
            long userId = Long.parseLong(String.valueOf(param));
            int dbIndex = (int) (userId % 10);
            
            return String.format("user%02d", dbIndex == 0 ? 10 : dbIndex);
        } else {
            return dsType; 
        }
        
    }
}

```
然后进行配置生效，一切都OK了~~~：
```xml
<bean id="dynamicDataSourceAspect" class="com.gdk.spring.datasource.DynamicDataSourceAspect">
	<property name="dataSourceTypeElectPolicy">
		<bean class="com.my.MyDataSourceTypeElectPolicy" />
	</property>
</bean>
```

# 4. 发现在配置多数据源时，除了 driver, url, username, password 这几个参数不一样外，其他的都是一样的大量重复配置，有没有什么方式可以减少这种重复配置？
