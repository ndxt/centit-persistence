package com.centit.framework.core.datasource;

import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.common.ParamName;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;

@Aspect
public class DynamicDataSourceAspect {

    /**
     * 注册 注入点
     */
    @Pointcut("@annotation(com.centit.framework.core.datasource.TargetDataSource)")
    public void electDataSourceAspect(){}


    public static Map<String, Object> getMethodDescription(JoinPoint joinPoint){
        Map<String, Object> map = new HashMap<>(10);

        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Parameter[] parameters = method.getParameters();
        Object[] arguments = joinPoint.getArgs();
        int nps = parameters.length;
        int nas = arguments.length;
        for(int i=0; i<nps && i<nas; i++){
            String paramName = parameters[i].getName();
            if(parameters[i].isAnnotationPresent(ParamName.class)){
                ParamName param =  parameters[i].getAnnotation(ParamName.class);
                paramName = param.value();
            }
            map.put(paramName,arguments[i]);
        }
        return map;
    }

    /**
     * 设置数据源类型，在一个线程中同时只能保持同一个数据源
     * @param joinPoint 切入点
     * @param targetDataSource targetDataSource 注解信息
     */
    @Before("electDataSourceAspect() && @annotation(targetDataSource)")
    public  void doBefore(JoinPoint joinPoint, TargetDataSource targetDataSource) {
        String targetDataSourceName = targetDataSource.value();
        if(StringUtils.isNotBlank(targetDataSourceName)){

            if(targetDataSource.mapByParameter()){
                Map<String, Object> paramsMap = getMethodDescription(joinPoint);
                targetDataSourceName =
                        StringBaseOpt.castObjectToString(
                                VariableFormula.calculate(targetDataSourceName,paramsMap));
            }
            DynamicDataSourceContextHolder.setDataSourceType(targetDataSourceName);
        }
    }



    /**
     * 清楚数据源，恢复为默认的数据源
     * @param joinPoint joinPoint 切入点
     * @param targetDataSource  TargetDataSource 注解
     * @param e 如果为null没有异常说明执行成功，否在记录异常信息
     */
    @AfterThrowing(pointcut = "electDataSourceAspect() && @annotation(targetDataSource)", throwing = "e")
    public  void doAfterThrowing(JoinPoint joinPoint, TargetDataSource targetDataSource, Throwable e) {
        if(StringUtils.isNotBlank(targetDataSource.value())){
            DynamicDataSourceContextHolder.clearDataSourceType();
        }
    }

    /**
     * 清楚数据源，恢复为默认的数据源
     * @param joinPoint joinPoint 切入点
     * @param targetDataSource  TargetDataSource 注解
     */
    @AfterReturning(pointcut = "electDataSourceAspect() && @annotation(targetDataSource)")
    public  void doAfterReturning(JoinPoint joinPoint, TargetDataSource targetDataSource) {
        if(StringUtils.isNotBlank(targetDataSource.value())){
            DynamicDataSourceContextHolder.clearDataSourceType();
        }
    }

}
