package com.centit.support.database.utils;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * 严格 SQL 注入结果。INVALID 状态绝不携带可执行的半成品 SQL。
 */
public record StrictSqlResult(Status status,
                              QueryAndNamedParams query,
                              StrictSqlReasonCode reasonCode,
                              List<StrictSqlAnchorDiagnostic> anchors,
                              List<StrictSqlFilterDiagnostic> filters) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public enum Status {
        READY,
        INVALID
    }

    public StrictSqlResult {
        anchors = anchors == null ? List.of() : List.copyOf(anchors);
        filters = filters == null ? List.of() : List.copyOf(filters);
        if (status == Status.INVALID) {
            query = null;
        }
    }

    public static StrictSqlResult ready(QueryAndNamedParams query,
                                        List<StrictSqlAnchorDiagnostic> anchors,
                                        List<StrictSqlFilterDiagnostic> filters) {
        return new StrictSqlResult(Status.READY, query, StrictSqlReasonCode.READY,
            anchors, filters);
    }

    public static StrictSqlResult invalid(StrictSqlReasonCode reasonCode,
                                          List<StrictSqlAnchorDiagnostic> anchors,
                                          List<StrictSqlFilterDiagnostic> filters) {
        return new StrictSqlResult(Status.INVALID, null, reasonCode, anchors, filters);
    }

    public boolean isReady() {
        return status == Status.READY;
    }
}
