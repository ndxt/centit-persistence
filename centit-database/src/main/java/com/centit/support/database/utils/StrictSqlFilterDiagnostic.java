package com.centit.support.database.utils;

import java.io.Serial;
import java.io.Serializable;

/**
 * 一条 filter 在一个锚点上的编译结果。
 */
public record StrictSqlFilterDiagnostic(String filterId,
                                        int anchorIndex,
                                        boolean compiled,
                                        StrictSqlReasonCode reasonCode) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
}
