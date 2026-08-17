package com.centit.support.database.utils;

/**
 * 一个 SQL 锚点的外置过滤决定：FULL 不注入过滤、FILTERED 注入过滤、DENY 注入恒假条件。
 */
public enum StrictSqlAccess {
    FULL,
    FILTERED,
    DENY
}
