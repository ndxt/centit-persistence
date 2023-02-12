# 先腾持久化平台PDSqlOrm使用手册
## 综述
先腾持久化平台PDSqlOrm（参数驱动SQL的ORM平台）的目标是最大限度的减少sql语句的编写，便于代码的重构，同时保持简单易学。 PDSqlOrm通过实现jpa注解的方式来减少sql语句的编写，其仅仅实现了jpa的部分注解：

    Column、Id、EmbeddedId、Basic、OrderBy、OneToOne、OneToMany、JoinColumns

另外扩展一个注解 ValueGenerator 和jpa的GeneratedValue不一样，它是一个数值生成器，可以作为插入和更新是的触发器。
 
一个PO示例

## 增删改

ValueGenerator 的用法

## 子表级联更新

## 查询条件的构造

## 增删改的增强
### 版本更新
Serializable （序列化）
数据库事务的最高隔离级别。在此级别下，事务串行执行。可以避免脏读、不可重复读、幻读等读现象。但是效率低下，耗费数据库性能，不推荐使用。
### 逻辑删除

### 字段类型扩展

## 数据范围权限

## 参数驱动sql语句
