# 概述
[江苏南大先腾J2EE持久化框架](https://github.com/ndxt/centit-persistence)研发目的不是为了取代MyBatis、Hibernate、Spring JDBC这样的成熟的持久化平台，而是为了让开发人员根容易的使用这些平台。这个框架一共有7个模块，其中一个共用模块，其他的分别是基于MyBatis、Hibernate、Spring JDBC开发的持久化框架和对应的spring 4 配置类。

设计这样持久化框架的目标有两个：

 1. 通过在Hibernate、MyBatis、Spring JDBC的基础上实现一些通用的方法，简化它们的使用难度，较少开发人员的学习成本。
 2. 通过对Hibernate、MyBatis、Spring JDBC的封装，让它们支持[参数驱动sql](https://blog.csdn.net/code_fan/article/details/81456580)， 然它们处理一些常见的场景拥有类似的方式，让不同喜好的开发人员可以更好的交流。当然这三个技术差别很大，框架中的特性也不是全部都能在它们之中无差别实现的。

对于Hibernate、MyBatis、Spring JDBC笔者认为Hibernate功能最完备，同时也是学习难度较大的，先腾持久化框架中笔者强力推荐Spring JDBC模块，因为这个模块式最灵活的，框架所有的特性支持的最好的，并且框架还通过[Spring jdbc对JPA的一个子集进行了实现](https://blog.csdn.net/code_fan/article/details/81387061)这样使用Spring jdbc就更加便捷了。

# 设计内容
先腾持久化设计的内容包括：

 1.  通用的分页查询。
 2.  多数据源支持。
 3.  数据范围权限支持
 4.  业务数据逻辑删除（MyBatis不支持这个特性）。
 5.  通用的增删改操作。
 6.  各种sql语句查询接口。
 7.  参数驱动sql的支持。
 8.  存储过程调用方式。
 9.  DDL 语句的支持。

  
