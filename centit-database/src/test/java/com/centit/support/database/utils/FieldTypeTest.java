package com.centit.support.database.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FieldTypeTest {

    @Test
    void postgresNativeTypeAliasesFoldToStandardNames() {
        String[][] aliases = {{"INT2", "SMALLINT"}, {"INT4", "INTEGER"}, {"INT8", "BIGINT"},
            {"FLOAT4", "REAL"}, {"FLOAT8", "DOUBLE PRECISION"}, {"BOOL", "BOOLEAN"}, {"BPCHAR", "CHAR"}};
        for (String[] names : aliases) {
            assertEquals(FieldType.mapToFieldType(names[1], DBType.PostgreSql),
                FieldType.mapToFieldType(names[0], DBType.PostgreSql));
        }
        assertEquals(FieldType.LONG, FieldType.mapToFieldType("int8", DBType.PostgreSql));
        assertEquals(FieldType.BOOLEAN, FieldType.mapToFieldType("BOOL", DBType.PostgreSql));
        assertEquals(FieldType.STRING, FieldType.mapToFieldType("bpchar", DBType.PostgreSql));
    }

    @Test
    void dialectAliasesAreNotFoldedOutsideTheirDialect() {
        // MySQL 下 INT8 不是 PostgreSQL 的 bigint 别名，仍按整型处理。
        assertNotEquals(FieldType.mapToFieldType("INT8", DBType.MySql),
            FieldType.mapToFieldType("BIGINT", DBType.MySql));
        assertEquals(FieldType.INTEGER, FieldType.mapToFieldType("INT8", DBType.MySql));
    }

    @Test
    void similarCustomTypeNamesAreNotFoldedByAliasMatching() {
        // 别名折叠是精确匹配，CUSTOM_INT8 不会被误折叠为 bigint。
        assertNotEquals(FieldType.mapToFieldType("BIGINT", DBType.PostgreSql),
            FieldType.mapToFieldType("CUSTOM_INT8", DBType.PostgreSql));
    }

    @Test
    void precisionSuffixAndModifiersAreStrippedBeforeMatching() {
        assertEquals(FieldType.STRING, FieldType.mapToFieldType("VARCHAR(255)", DBType.MySql));
        assertEquals(FieldType.NUMBER, FieldType.mapToFieldType("DECIMAL(10,2)", DBType.Oracle));
        assertEquals(FieldType.TEXT, FieldType.mapToFieldType("CLOB(1024)", DBType.H2));
        assertEquals(FieldType.LONG, FieldType.mapToFieldType("BIGINT UNSIGNED", DBType.MySql));
        assertEquals(FieldType.LONG, FieldType.mapToFieldType("BIGINT AUTO_INCREMENT", DBType.MySql));
    }

    @Test
    void h2FloatIsDoubleWhileOtherDialectsKeepFloat() {
        assertEquals(FieldType.DOUBLE, FieldType.mapToFieldType("FLOAT", DBType.H2));
        assertEquals(FieldType.FLOAT, FieldType.mapToFieldType("FLOAT", DBType.MySql));
    }

    @Test
    void clobAndBinaryFamiliesCollapseToSemanticTypes() {
        assertEquals(FieldType.TEXT, FieldType.mapToFieldType("CHARACTER LARGE OBJECT", DBType.H2));
        assertEquals(FieldType.TEXT, FieldType.mapToFieldType("LONGTEXT", DBType.MySql));
        assertEquals(FieldType.TEXT, FieldType.mapToFieldType("MEDIUMTEXT", DBType.MySql));
        assertEquals(FieldType.TEXT, FieldType.mapToFieldType("NCLOB", DBType.Oracle));
        assertEquals(FieldType.BYTE_ARRAY, FieldType.mapToFieldType("BINARY LARGE OBJECT", DBType.H2));
        assertEquals(FieldType.BYTE_ARRAY, FieldType.mapToFieldType("BINARY VARYING", DBType.PostgreSql));
    }

    @Test
    void nullTypeStaysNull() {
        assertNull(FieldType.mapToFieldType(null, DBType.MySql));
    }
}
