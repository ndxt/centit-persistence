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
<bean id="dynamic_datasource" class="com.centit.framework.core.datasource.DynamicDataSource">
    <!-- 注册目标多个数据源 -->
	<property name="targetDataSources">  
		<map key-type="java.lang.String">
		   <!--这里的key值将是 @TargetDataSource("...") 的值-->  
		   <entry key="ds1" value-ref="ds0"/> 
		   <entry key="ds2" value-ref="ds1"/>
		   <entry key="ds2" value-ref="ds2"/>
		   <entry key="ds2" value-ref="ds3"/>
		</map>  
    </property>
    <!-- 如果出现了未知的数据源，则将使用这里配置的默认数据源替代 -->
    <property name="defaultTargetDataSource" ref="ds1"/>  
</bean>
```

步骤3： 配置生效 `DynamicDataSourceAspect` AOP切面拦截器。
```xml
<bean id="dynamicDataSourceAspect" class="com.centit.framework.core.datasource.DynamicDataSourceAspect">
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
    
    @TargetDataSource("ds0")
    public List<OrderItem> findOrderItems() {
        // ...
    }
}
```

```java
/** 
 * 根据参数动态的计算数据源
 */
@Service("shoppingCartService")
public class ShoppingCartService {

    public List<ShoppingItem> findShoppingItems() {
        // ...
    }    

    @TargetDataSource(value = "'ds'+ (userId mod 4)", mapByParameter = true)
    public Address getDefaultDeliverAddress(long userId) {
        // ...
    }
}
