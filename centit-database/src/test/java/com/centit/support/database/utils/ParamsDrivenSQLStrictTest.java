package com.centit.support.database.utils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParamsDrivenSQLStrictTest {

    @Test
    void filteredDecisionInjectsRequiredAnchorWithNamespacedParameter() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(filter("owner", "[orders.owner_code] = {currentUser}")),
            Map.of("currentUser", "U001"));

        assertTrue(result.isReady());
        assertEquals("select * from orders o where 1=1  and o.owner_code = :__rs_a0_f0_currentUser",
            result.query().getQuery());
        assertEquals(Map.of("__rs_a0_f0_currentUser", "U001"), result.query().getParams());
        assertEquals(1, result.anchors().size());
        assertTrue(result.anchors().get(0).covered());
    }

    @Test
    void multipleFiltersUseIndependentParameterNamespaces() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(
                filter("owner", "[orders.owner_code] = {value}"),
                filter("creator", "[orders.creator_code] = {value}")),
            Map.of("value", "U001"));

        assertTrue(result.isReady());
        assertTrue(result.query().getQuery().contains(":__rs_a0_f0_value"));
        assertTrue(result.query().getQuery().contains(":__rs_a0_f1_value"));
        assertEquals(2, result.query().getParams().size());
    }

    @Test
    void invalidFilterRejectsWholeResultWithoutExecutableSql() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(
                filter("valid", "[orders.owner_code] = {currentUser}"),
                filter("missing-variable", "[orders.unit_code] = {missing}")),
            Map.of("currentUser", "U001"));

        assertEquals(StrictSqlResult.Status.INVALID, result.status());
        assertEquals(StrictSqlReasonCode.FILTER_PARTIALLY_COMPILED, result.reasonCode());
        assertNull(result.query());
        assertFalse(result.filters().get(1).compiled());
    }

    @Test
    void unrelatedFilterIsNotApplicableButApplicableFilterStillCoversAnchor() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(
                filter("customer", "[customer.owner_code] = {currentUser}"),
                filter("order", "[orders.owner_code] = {currentUser}")),
            Map.of("currentUser", "U001"));

        assertTrue(result.isReady());
        assertEquals(StrictSqlReasonCode.FILTER_NOT_APPLICABLE,
            result.filters().get(0).reasonCode());
        assertEquals(1, result.anchors().get(0).compiledFilterCount());
    }

    @Test
    void allNotApplicableFiltersLeaveRequiredAnchorUncovered() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(filter("customer", "[customer.owner_code] = {currentUser}")),
            Map.of("currentUser", "U001"));

        assertEquals(StrictSqlReasonCode.REQUIRED_ANCHOR_UNCOVERED, result.reasonCode());
        assertNull(result.query());
    }

    @Test
    void filteredSqlWithoutRequiredAnchorIsInvalid() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(filter("owner", "[orders.owner_code] = {currentUser}")),
            Map.of("currentUser", "U001"));

        assertEquals(StrictSqlReasonCode.REQUIRED_ANCHOR_MISSING, result.reasonCode());
        assertNull(result.query());
    }

    @Test
    void fullDecisionRemovesAnchorWithoutAddingPredicate() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.FULL, List.of(), Map.of());

        assertTrue(result.isReady());
        assertEquals("select * from orders o where 1=1 ", result.query().getQuery());
        assertTrue(result.query().getParams().isEmpty());
    }

    @Test
    void denyDecisionCompilesRequiredAnchorToFalse() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.DENY, List.of(), Map.of());

        assertTrue(result.isReady());
        assertTrue(result.query().getQuery().contains("and 0=1"));
    }

    @Test
    void selfJoinCanAddressEachAliasIndependently() {
        StrictSqlResult result = translate(
            "select * from orders buyer join orders seller on buyer.parent_id=seller.id "
                + "where 1=1 {required orders:buyer,orders:seller}",
            StrictSqlAccess.FILTERED,
            List.of(
                filter("buyer", "[buyer.owner_code] = {userCode}"),
                filter("seller", "[seller.owner_code] = {userCode}")),
            Map.of("userCode", "U001"));

        assertTrue(result.isReady());
        assertTrue(result.query().getQuery().contains("buyer.owner_code"));
        assertTrue(result.query().getQuery().contains("seller.owner_code"));
        assertEquals(List.of("orders:buyer", "orders:seller"),
            result.anchors().get(0).tableReferences());
    }

    @Test
    void selfJoinTableNameWithoutAliasIsAmbiguousAndUncovered() {
        StrictSqlResult result = translate(
            "select * from orders buyer join orders seller on buyer.parent_id=seller.id "
                + "where 1=1 {required orders:buyer,orders:seller}",
            StrictSqlAccess.FILTERED,
            List.of(filter("ambiguous", "[orders.owner_code] = {userCode}")),
            Map.of("userCode", "U001"));

        assertEquals(StrictSqlReasonCode.REQUIRED_ANCHOR_UNCOVERED, result.reasonCode());
        assertNull(result.query());
    }

    @Test
    void emptyCollectionAndObjectArrayCompileToSafeFalse() {
        for (Object empty : List.of(List.of(), new Object[0])) {
            StrictSqlResult result = translate(
                "select * from orders o where 1=1 {required orders:o}",
                StrictSqlAccess.FILTERED,
                List.of(filter("units", "[orders.unit_code] in ({(creepForIn)units})")),
                Map.of("units", empty));

            assertTrue(result.isReady());
            assertTrue(result.query().getQuery().contains("and 0=1"));
            assertTrue(result.query().getParams().isEmpty());
        }
    }

    @Test
    void malformedSqlTemplateAndFilterAreInvalid() {
        StrictSqlResult malformedAnchor = translate(
            "select * from orders where 1=1 {required orders",
            StrictSqlAccess.FILTERED,
            List.of(filter("owner", "[orders.owner] = {user}")), Map.of("user", "U1"));
        assertEquals(StrictSqlReasonCode.MALFORMED_TEMPLATE, malformedAnchor.reasonCode());

        StrictSqlResult malformedFilter = translate(
            "select * from orders where 1=1 {required orders}",
            StrictSqlAccess.FILTERED,
            List.of(filter("owner", "[orders.owner = {user}")), Map.of("user", "U1"));
        assertEquals(StrictSqlReasonCode.FILTER_PARTIALLY_COMPILED,
            malformedFilter.reasonCode());
        assertNull(malformedFilter.query());
    }

    @Test
    void duplicateFilterIdsAndDuplicateAliasesAreInvalid() {
        StrictSqlResult duplicateFilter = translate(
            "select * from orders where 1=1 {required orders}",
            StrictSqlAccess.FILTERED,
            List.of(
                filter("same", "[orders.owner] = {user}"),
                filter("same", "[orders.creator] = {user}")), Map.of("user", "U1"));
        assertEquals(StrictSqlReasonCode.FILTER_ID_DUPLICATED, duplicateFilter.reasonCode());

        StrictSqlResult duplicateAlias = translate(
            "select * from orders a join customer a on 1=1 {required orders:a,customer:a}",
            StrictSqlAccess.FILTERED,
            List.of(filter("owner", "[a.owner] = {user}")), Map.of("user", "U1"));
        assertEquals(StrictSqlReasonCode.ANCHOR_INVALID, duplicateAlias.reasonCode());
    }

    @Test
    void conflictingReuseOfOneAliasWithinFilterIsRejected() {
        StrictSqlResult result = translate(
            "select * from orders o where 1=1 {required orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(filter("conflict",
                "[orders.owner] = {owner:shared} or [orders.creator] = {creator:shared}")),
            Map.of("owner", "U1", "creator", "U2"));

        assertEquals(StrictSqlReasonCode.FILTER_PARTIALLY_COMPILED, result.reasonCode());
        assertNull(result.query());
    }

    @Test
    void filterParameterCannotOverwriteExistingNamedParameter() {
        StrictSqlResult result = translate(
            "select * from orders o where o.status=:__rs_a0_f0_user "
                + "{required orders:o}",
            StrictSqlAccess.FILTERED,
            List.of(filter("owner", "[orders.owner] = {user}")),
            Map.of("user", "U1"));

        assertEquals(StrictSqlReasonCode.PARAMETER_COLLISION, result.reasonCode());
        assertNull(result.query());
    }

    @Test
    void collectionValueWithoutPretreatmentExpandsToInParameterList() {
        for (Object units : List.of(List.of("U1", "U2"), new Object[] {"U1", "U2"})) {
            StrictSqlResult result = translate(
                "select * from orders o where 1=1 {required orders:o}",
                StrictSqlAccess.FILTERED,
                List.of(filter("units", "[orders.unit_code] in ({units})")),
                Map.of("units", units));

            assertTrue(result.isReady());
            // 未标注 creepForIn 的集合值也应自动展开为 in 列表参数，而非单参数绑定整个集合
            assertEquals(2, result.query().getParams().size());
            assertTrue(result.query().getParams().containsValue("U1"));
            assertTrue(result.query().getParams().containsValue("U2"));
            assertTrue(result.query().getQuery().contains("unit_code"));
        }
    }

    @Test
    void legacyTranslateQueryExpandsCollectionAndEmptyCollectionBindsNull() {
        // legacy 全路径（锚点解析 + translateQueryFilter）：集合自动展开
        QueryAndNamedParams expanded = ParamsDrivenSQL.translateQuery(
            "select * from orders o where 1=1 {orders:o}",
            List.of("[orders.unit_code] in ({units})"), true,
            new ParamsDrivenSQL.SimpleFilterTranslate(Map.of("units", List.of("U1", "U2"))));
        assertNotNull(expanded);
        assertEquals(2, expanded.getParams().size());
        assertTrue(expanded.getQuery().contains("unit_code"));

        // legacy 路径空集合：占位符替换为 null 字面量（in (null) 恒不匹配 = 空集语义）
        QueryAndNamedParams empty = ParamsDrivenSQL.translateQuery(
            "select * from orders o where 1=1 {orders:o}",
            List.of("[orders.unit_code] in ({units})"), true,
            new ParamsDrivenSQL.SimpleFilterTranslate(Map.of("units", List.of())));
        assertNotNull(empty);
        assertTrue(empty.getQuery().contains("null"), "空集合应替换为 null: " + empty.getQuery());
        assertTrue(empty.getParams().isEmpty());
    }

    private static StrictSqlFilter filter(String id, String expression) {
        return new StrictSqlFilter(id, expression);
    }

    private static StrictSqlResult translate(String sql,
                                             StrictSqlAccess access,
                                             List<StrictSqlFilter> filters,
                                             Map<String, Object> variables) {
        return ParamsDrivenSQL.translateQueryStrict(sql, access, filters, true,
            new ParamsDrivenSQL.SimpleFilterTranslate(variables));
    }
}
