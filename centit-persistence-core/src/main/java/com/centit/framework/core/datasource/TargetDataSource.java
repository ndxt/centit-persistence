package com.centit.framework.core.datasource;

import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

@Target({ElementType.METHOD,ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TargetDataSource {
    /**
     * 数据源名称
     * @return 数据源名称
     */
    @AliasFor("name")
    String value() default "";

    @AliasFor("value")
    String name() default "";
}
