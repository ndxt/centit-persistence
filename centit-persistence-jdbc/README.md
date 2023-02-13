# 先腾持久化平台PDSqlOrm使用手册
## 综述

先腾持久化平台PDSqlOrm（参数驱动的ORM平台）的目标是最大限度的减少sql语句的编写，便于代码的重构，同时保持简单易学。 PDSqlOrm是基于Spring jdbc研发的，相关的事务处理都是通过spring-tx实现的，没有什么特别之处。它通过实现jpa注解的方式来减少sql语句的编写，其仅仅实现了jpa的部分注解：

    Column、Id、EmbeddedId、Basic、OrderBy、OneToOne、OneToMany、JoinColumns

另外扩展一个注解 ValueGenerator 和jpa的GeneratedValue不一样，它是一个数值生成器，可以作为插入和更新是的触发器。
 
PDSqlOrm的核心类有两个，一个是[BaseDaoImpl](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/BaseDaoImpl.java)、另一个是[参数驱动sql语句执行类DatabaseOptUtils](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/main/java/com/centit/framework/jdbc/dao/DatabaseOptUtils.java)。
平台提供的一个员工信息表包括员工的工作经历子表示例：[PO对象类](https://github.com/ndxt/centit-persistence/tree/master/centit-persistence-jdbc/src/test/java/com/centit/framework/jdbc/test/po)，对应的[测试类](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/test/java/com/centit/framework/jdbc/test/server/TestClassTemp.java)。

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

### 逻辑删除

Po对象如果实现 [EntityWithDeleteTag](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/main/java/com/centit/framework/core/po/EntityWithDeleteTag.java) 接口在调用Dao的deleteObject**方法时，数据不会正真被删除，而是通过接口中setDeleted方法修改对应的标记字段。
开发人员可以自行设计标记删除状态的方法，从而实现逻辑删除。如果用户想从数据库中彻底删除数据需要调用deleteObjectForce*方法，或者执行运行delete sql语句。

### 版本更新

数据库事务的最高隔离级别Serializable（序列化）因为效率低下，一般不推荐使用。但是实际需求还是有的，比如：有个属性当它的值为a时甲看到会更改为b、如果是c会更改为d，乙看到它时a时会更改为c、b会更改为d。 如果现在数据库中这个属性是a，甲和乙都看到了，他们每人修改了一次，这是数据库中的值 可能是b或者c；这是有不确定的，后面的人会覆盖前面的更改，也不是业务所期望的，根据业务这个值应该是d。
为了避免这样情况发生是可以通过软件来控制的。PDSqlOrm中的Po对象如果实现 [EntityWithVersionTag](https://github.com/ndxt/centit-persistence/blob/master/centit-persistence-jdbc/src/main/java/com/centit/framework/core/po/EntityWithVersionTag.java)接口，在调用updateObject*接口时，系统会校验数据库中数据的版本信息，如果一致则更改数据同时更改版本信息如果不一致则不会更新，通过返回值0或者1来反馈是否更新成功。
另外：deleteObject** 也会校验版本信息，同样通过返回删除条目的数量来反馈是否删除成功。

## 查询条件的构造

PDSqlOrm命名为参数驱动的ORM平台，包括两部分：参数驱动的查询和参数驱动的SQL；是这个Orm平台的核心部分，也是通过参数驱动查询来减少sql语句的编写和从而能够最大化的提高跨数据库平台的能力。
这一节通过上面提到的具体的示例来展示PDSqlOrm的用法。

### 过滤参数

比如我们需要查询姓名为"张三"的员工，可以通过一下代码：

    officeWorkerDao.listObjectsByProperties(CollectionsOpt.createHashMap("workerName", "张三"));

这里的workerName为Po对象中对应的属性名（直接用字段名也可以），接口会在后台编译为 "WORKER_NAME = '张三'" 的查询过滤语句，但是如果我们想用其他的关系表达式怎么办呢？ 可以通过参数名添加后缀来解决，比如：

    officeWorkerDao.listObjectsByProperties(CollectionsOpt.createHashMap(
                "workerBirthday_ge", DatetimeOpt.createUtilDate(1990,1,1)，
                "workerBirthday_lt", DatetimeOpt.createUtilDate(2000,1,1)));

这个示例查询上世纪90年代出生的员工，编译为 WORKER_BIRTHDAY >= "1900-01-01" and ORKER_BIRTHDAY < "2000-01-01" 的查询条件。平台提供以下后缀表达式：

**eq** 等于， **ge** 大于等于，**gt** 大于，**lt** 小于，**le** 小于等于，**ne** 不等于，**lk** like， **in** 包括in(列表)， **ni** 不包含，not in（列表），**nv** is null， **nn** is not null

如果没有后缀则视为eq。

### 内置过滤参数

开发过程中碰到的查询有时会比这个复杂的多, 比如想查询跳槽三次的员工，是需要通过子查询才能解决的，可以通过内置过滤参数来实现，需要重载Dao类的getFilterField方法：

    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        ......
        filterField.put("tiaoChaoSanCiYiShang", 
                "WORKER_ID in (select a.WORKER_ID from T_CAREER a group by a.WORKER_ID having count(*)>3)");
        return filterField;
    }

调用的时候使用下面的代码，因为这个条件不需要参数，所以值可以是任意不为null的对象。

    officeWorkerDao.listObjectsByProperties(CollectionsOpt.createHashMap("tiaoChaoSanCiYiShang", "Any Not Null Object"));

这个getFilterField方法中的条件可以和上面参数后缀逻辑可以混合使用，它们产生的过滤条件都是通过 and 连接，如果想使用 or 来连接就需要对参数进行分组了。

### 参数分组

过滤参数可以通过前缀来进行分组，同一分组内的逻辑用 or 连接，不同分组和没有分组的参数用 and 连接。
前缀的格式为 字母 g + 一个分组数字0～9（最多支持10个分组） + 一个可选的辅助字符母（可以下滑线_以外是任意可见字符）+ 下滑线_ 构成。
比如需要查询 15岁以下或者65以上，姓名为张三或者李四（不用in）的人，查询方式如下：

    officeWorkerDao.listObjectsByProperties(CollectionsOpt.createHashMap(
            "g0_workerAge_lt", 15，
            "g0_workerAge_gt", 65,
            "g2a_workerName", "张三"，
            "g2b_workerName", "李四"));

查询接口listObjectsByProperties传入的参数是map，可选的辅助字符母主要目的避免相同的逻辑传入map的key重复的问题。
上面的示例对应的过滤条件为 （WORKER_AGE < 15 or WORKER_AGE >65) and (WORKER_NAME = '张三'  or WORKER_NAME = '李四') 。

### 参数预处理



## 数据范围权限



## 参数驱动sql语句



## 总结
如何简化 查询语句。
除了统计分析语句不推荐执行运行sql语句。
