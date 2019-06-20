package com.centit.framework.core.datasource;

/**
 * 参考 烽火科技 王永山 的设计，在此表示感谢
 * 同时参考了github上的 spring-multi-datasource 项目
 */
public class DynamicDataSourceContextHolder {
    private static final ThreadLocal<String> contextHolder = new ThreadLocal<>(); // 线程本地环境

    // 设置数据源类型
    public static void setDataSourceType(String dataSourceType){
        contextHolder.set(dataSourceType);
    }

    // 获取数据源类型
    public static String getDataSourceType(){
        return contextHolder.get();
    }

    // 清除数据源类型
    public static void clearDataSourceType(){
        contextHolder.remove();
    }
}
