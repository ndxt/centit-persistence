# 先腾持久化平台PDSqlOrm使用手册
## 综述
先腾持久化平台PDSqlOrm（参数驱动SQL的ORM平台）的目标是最大限度的减少sql语句的编写，便于代码的重构，同时保持简单易学。 PDSqlOrm是基于Spring jdbc研发的，相关的事务处理都是通过spring-tx实现的，没有什么特别之处。它通过实现jpa注解的方式来减少sql语句的编写，其仅仅实现了jpa的部分注解：

    Column、Id、EmbeddedId、Basic、OrderBy、OneToOne、OneToMany、JoinColumns

另外扩展一个注解 ValueGenerator 和jpa的GeneratedValue不一样，它是一个数值生成器，可以作为插入和更新是的触发器。
 
PDSqlOrm的核心类有两个，一个是[BaseDaoImpl](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/BaseDaoImpl.java)、另一个是[参数驱动sql语句执行类DatabaseOptUtils](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/DatabaseOptUtils.java)。平台提供的一个示例[PO对象类](https://github.com/ndxt/centit-persistence/tree/master/centit-persistence-jdbc/src/test/java/com/centit/framework/jdbc/test/po)，对应的[测试类](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/test/java/com/centit/framework/jdbc/test/server/TestClassTemp.java)。

## 增删改
PDSqlOrm是通过jpa注解实现对象和数据的映射的，但是和标准的jpa有有点差别，所以这边我们以一个具体的示例来说明PDSqlOrm的用法。

    /**
    * 定一个po对象OfficeWorker，其中包括对应的jpa注解，定一个 OfficeWorkerDao，简单的扩展一下BaseDaoImpl就可以
    * 轻松的实现对象的增删改查了。
    */
    public class OfficeWorkerDao extends BaseDaoImpl<OfficeWorker, String> { ...... }

下面仅仅说明和jpa有差别的地方（主要是和hibernate对比）。

### 字段类型扩展
PDSqlOrm添加了对Boolean、枚举型Enum、java对象和JSONObject。

**Boolean**类型，会持久化到数据库中，统一为T/F， 读取时会将 t、true、yes、y、1 、on转换为 true，将 f、false、no、n、0 、off转换为 false（不区分大小写）。
**枚举型Enum**类型，保存的是枚举型的序号，所以如果枚举型重构，新的类型放在最下面。
**java对象和JSONObject**，数据库中会存储json对象，所以需要注意长度可以使用clob（text）类型字段。写入和读取时会自动转换类型，但是不支持数组（Collection、array）。

### 字段数值生成器 ValueGenerator
ValueGenerator 和 jpa 的GeneratedValue不一样，GeneratedValue只是标注这个字段会自动生成，并没有说明生成方式，生成方式还需要另行定义。

ValueGenerator 是一个数值生成器，可以用于任意字段，不仅仅是主键上。它有4个属性：
 * strategy 和 value ，前者生成策略，后者是生成参数，这个参数不是必须的。生成策略参见[GeneratorType](https://github.com/ndxt/centit-persistence/blob/master/centit-database/src/main/java/com/centit/support/database/orm/GeneratorType.java)枚举类的注释。
 * occasion 生成事件，新增、修改、读取 和 新增与修改组合。
 * condition 生成条件，是在数值为null时生成，还是一直生成。

一个示例：对象的lastUpdateTime在每次修改时自动修改为当前日期

    @Column(name = "LAST_UPDATE_DATE")
    @ValueGenerator( strategy= GeneratorType.FUNCTION, value = "today()",
                        condition = GeneratorCondition.ALWAYS, occasion = GeneratorTime.NEW_UPDATE )
    private Date lastUpdateTime;

### 数据关联（子表）

PDSqlOrm只实现了两个关于字表的注解OneToOne和OneToMany。和hibernate不同的是它永远不会自动的获取字表，比如在调用 getObjectById 返回的对象对应子表的属性永远为null，如果需要获取对应的数值需要调用 getObjectWithReferences 来获取对象，或者获取对象后通过 fetchObjectReferences 或者 fetchObjectReference 来加载子表。

### 数据查询与懒加载
懒加载一般用于lob字段上，用于提高效率。PDSqlOrm通过 @Basic(fetch = FetchType.LAZY) 注解来标识懒加载字段。在获取单个对象的方法（getObject*）中默认都是会默认加载这些Lazy字段的，在获取列表的方法（listObjets**）中都是默认不加这些字段的。
如果获取对象时不想加载Lazy字段需要调用 getObjectExcludeLazyById 方法，如果查询列表时想包括lzay字段 需要调用 listObjectsPartFieldByPropertiesAsJson 通过 fields 指定所有的字段，可以通过 DatabaseOptUtils.extraPoAllFieldNames(poClass.class) 获取某个对象上的字段。
另外，listObjects**方法永远不会加载子表，如果需要加载子表需要循环调用上面提到的fetchObjectReference(s)方法。

### 版本更新
Serializable （序列化）
数据库事务的最高隔离级别。在此级别下，事务串行执行。可以避免脏读、不可重复读、幻读等读现象。但是效率低下，耗费数据库性能，不推荐使用。
### 逻辑删除


## 查询条件的构造



## 数据范围权限

## 参数驱动sql语句
