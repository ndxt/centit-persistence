package com.centit.support.database.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.Serial;
import java.io.Serializable;

/**
 * 严格 SQL 注入使用的一条可追踪过滤条件。
 *
 * @param filterId   调用方提供的稳定 ID，用于诊断和参数命名空间
 * @param expression 外置过滤表达式（调用方语义，本库只负责编译注入）
 */
public record StrictSqlFilter(String filterId, String expression) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public StrictSqlFilter {
        if (StringUtils.isBlank(filterId)) {
            throw new IllegalArgumentException("filterId cannot be blank");
        }
        if (StringUtils.isBlank(expression)) {
            throw new IllegalArgumentException("filter expression cannot be blank");
        }
    }
}
