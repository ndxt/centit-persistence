package com.centit.framework.core.po;

/**
 * PO实体实现这个接口，
 * 用于版本根新
 * 目前只有jdbc 中实现这个接口
 */
public interface EntityWithVersionTag {
    /**
     * 计算下一个版本号，版本号可以为任何类型，但是必须支持sql语句中的 =
     * @return 下一个版本号
     */
    Object calcNextVersion();
    /**
     * 返回记录版本的属性，这个属性必须和数据库表中的某个字段对应，
     * 也就是说这个属性必须有 @Column
     * @return 版本字段属性名
     */
    String obtainVersionProperty();

}
