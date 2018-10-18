package com.centit.framework.mybatis.plugin;

import com.centit.support.compiler.Lexer;
import com.centit.support.database.utils.QueryUtils;

import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Intercepts({
    @Signature(
        type = StatementHandler.class,
        method = "prepare",
        args = {Connection.class, Integer.class}
    )/*,
    @Signature(
        type = ParameterHandler.class,
        method = "prepare",
        args = {Connection.class, Integer.class}
    )*/
})
public class ParameterDriverSqlInterceptor implements Interceptor {

    private static final String PARAMETER_DRIVER_SQL_SUFFIX ="ByPDSql";

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        // 获取StatementHandler，默认是RoutingStatementHandler
        StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
        // 获取statementHandler包装类
        MetaObject metaStmtHandler = SystemMetaObject.forObject(statementHandler);

        //----------- 循环遍历找到最终被代理的真实对象 -----------
        // JDK 自身的动态代理对象字段名称为: h
        while (metaStmtHandler.hasGetter("h")) {
            Object obj = metaStmtHandler.getValue("h");
            metaStmtHandler = SystemMetaObject.forObject(obj);
        }

        // 其它代理框架（CGLIB 和 javassist）代理对象字段名称：target
        while (metaStmtHandler.hasGetter("target")) {
            Object obj = metaStmtHandler.getValue("target");
            metaStmtHandler = SystemMetaObject.forObject(obj);
        }
        //-----------------------------------------------------

        // 获取StatementHandler的实现类
        //metaObject.getValue("delegate");

        // 通过反射获取查询接口映射的相关信息
        MappedStatement mappedStatement = (MappedStatement) metaStmtHandler.getValue("delegate.mappedStatement");

        BoundSql boundSql = statementHandler.getBoundSql();
        //boundSql.getParameterMappings();
        String nativeSql = boundSql.getSql(); // (String)  metaStmtHandler.getValue("delegate.boundSql.sql");

        String mapId = mappedStatement.getId();
        boolean isPDSql = mapId.endsWith(PARAMETER_DRIVER_SQL_SUFFIX);
        if(!isPDSql) {
            Lexer lexer = new Lexer(nativeSql, Lexer.LANG_TYPE_SQL);
            String aword = lexer.getARegularWord();
            if("--".equals(aword) || "/*".equals(aword)){
                aword = lexer.getARegularWord();
                if("PDSql".equals(aword)){
                    isPDSql = true;
                }else if("parameter".equalsIgnoreCase(aword)
                         || "parameters".equalsIgnoreCase(aword)){
                    aword = lexer.getARegularWord();
                    if("driver".equalsIgnoreCase(aword)) {
                        aword = lexer.getARegularWord();
                        if("sql".equalsIgnoreCase(aword)) {
                            isPDSql = true;
                        }
                    }
                }
            }
        }
        if(!isPDSql){
            return invocation.proceed();
        }

        Map<String,Object> parameterObjectMap =
                QueryAndParamMaps.parameterObjectMap(boundSql.getParameterObject());

        Object filter = parameterObjectMap.get("powerFilter");
        if(filter!=null){
            filter = parameterObjectMap.get("powerFilters");
        }
        QueryAndParamMaps qandP = QueryAndParamMaps.createFromQueryAndNamedParams(
                QueryUtils.translateQuery(nativeSql,
                    QueryAndParamMaps.objectToStringList(filter),parameterObjectMap, true ));


        // 通过反射将构建完成的分页sql语句赋值回去 'delegate.boundSql.sql' parameterMappings  parameterObject
        metaStmtHandler.setValue("delegate.boundSql.sql", qandP.getQuery());
        List<ParameterMapping> parameterMappings = new ArrayList<>(qandP.getParamNames().size());
        for(String sname : qandP.getParamNames()){
            parameterMappings.add(
                    new ParameterMapping.Builder(mappedStatement.getConfiguration(),
                            sname, Object.class).build());
        }
        metaStmtHandler.setValue("delegate.boundSql.parameterMappings", parameterMappings);
        /**
         * 添加新产生的变量
         */
        parameterObjectMap.putAll(qandP.getNamedParams());
        metaStmtHandler.setValue("delegate.boundSql.parameterObject", parameterObjectMap );

        return invocation.proceed();
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
