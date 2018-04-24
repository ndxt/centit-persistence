# 参数驱动sql Mybatis插件

Mybatis使用Mapper.XML来配置sql语句，插件ParameterDriverSqlInterceptor在Mybatis执行钱对sql语句进行预处理。

更多信息参见[参数驱动sql](https://ndxt.github.io/system_design/technical_design.html#%E5%8F%82%E6%95%B0%E9%A9%B1%E5%8A%A8sql)。

## 插件的启动方式

Mapper.XML中的语句ID（方法名称）如果以 ByPDSql 结尾，将会触发语句预处理活动。

如果不方便修改SQL语句名称，也可以在语句的最前面添加注释来触发， 比如：

```sql92
 -- PDSql
 /* PDSql */
 -- parameter driver sql 
 /* parameter driver sql */
-- parameters driver sql 
 /* parameters driver sql */
```
6中书写方式都可以，但是注释必须在语句的最前面。


## 配置方式 

```java
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Bean
    public SqlSessionFactoryBean sqlSessionFactory(@Autowired DataSource dataSource) throws IOException {
        SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        org.apache.ibatis.session.Configuration configuration = new org.apache.ibatis.session.Configuration();
        ......
        //添加数据权限拦击器
        configuration.addInterceptor(new ParameterDriverSqlInterceptor());
        ......
        sessionFactory.setConfiguration(configuration);
        ......
    }

```