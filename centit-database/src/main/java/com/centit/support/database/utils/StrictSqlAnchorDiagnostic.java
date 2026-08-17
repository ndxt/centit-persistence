package com.centit.support.database.utils;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * 一个外置过滤锚点的覆盖结果。
 */
public record StrictSqlAnchorDiagnostic(int anchorIndex,
                                        boolean required,
                                        List<String> tableReferences,
                                        int compiledFilterCount,
                                        boolean covered,
                                        StrictSqlReasonCode reasonCode) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public StrictSqlAnchorDiagnostic {
        tableReferences = tableReferences == null ? List.of() : List.copyOf(tableReferences);
    }
}
