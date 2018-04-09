package com.centit.framework.core.datasource;

import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;

@Aspect
public class DynamicDataSourceAspect {

    /**
     * 注册 注入点
     */
    @Pointcut("@annotation(com.centit.framework.core.datasource.TargetDataSource)")
    public void electDataSourceAspect(){}

    /**
     * 设置数据源类型，在一个线程中同时只能保持同一个数据源
     * @param joinPoint 切入点
     * @param targetDataSource targetDataSource 注解信息
     */
    @Before("electDataSourceAspect() && @annotation(targetDataSource)")
    public  void doBefore(JoinPoint joinPoint, TargetDataSource targetDataSource) {
        if(StringUtils.isNotBlank(targetDataSource.value())){
            DynamicDataSourceContextHolder.setDataSourceType(targetDataSource.value());
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
