package com.centit.support.database.ddl;

import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.FieldType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TableFieldEquivalenceTest {

    private SimpleTableField field(String columnType, String fieldType, Integer length, Integer scale) {
        SimpleTableField field = new SimpleTableField();
        field.setColumnName("AMOUNT");
        field.setColumnType(columnType);
        field.setFieldType(fieldType);
        if (length != null) field.setMaxLength(length);
        if (scale != null) field.setScale(scale);
        return field;
    }

    @Test
    void postgresNativeAliasFoldsToTheSamePhysicalType() {
        assertTrue(TableFieldEquivalence.sameDatabaseType(
            field("BIGINT", FieldType.LONG, 18, 0), field("int8", FieldType.LONG, 19, 0), DBType.PostgreSql));
        assertTrue(TableFieldEquivalence.sameDatabaseType(
            field("VARCHAR(20)", FieldType.STRING, 20, 0), field("bpchar", FieldType.STRING, 20, 0), DBType.PostgreSql));
    }

    @Test
    void h2NormalizesNonpositiveDecimalScaleToItsEmittedPrecision() {
        // H2 DDL 发射 DECIMAL(24,0)：负标度哨兵按 (24,0) 比较。
        assertTrue(TableFieldEquivalence.sameDatabaseType(
            field("DECIMAL", FieldType.NUMBER, 0, -127), field("DECIMAL(24,0)", FieldType.NUMBER, 24, 0), DBType.H2));
        assertEquals(24, TableFieldEquivalence.targetLength(field("DECIMAL", FieldType.NUMBER, 0, -127), DBType.H2));
        assertEquals(0, TableFieldEquivalence.targetScale(field("DECIMAL", FieldType.NUMBER, 0, -127), DBType.H2));
    }

    @Test
    void meaningfulNegativeScalesStayDistinctOutsideH2() {
        assertFalse(TableFieldEquivalence.sameDatabaseType(
            field("NUMBER", FieldType.NUMBER, 12, -2), field("NUMBER", FieldType.NUMBER, 12, 0), DBType.Oracle));
    }

    @Test
    void postgresUnboundedNumericSentinelsCollapseToZeroPrecision() {
        // PG JDBC 无约束 NUMERIC 回读（length 131089/scale 0 或 length 0）与显式 (0,0) 等价。
        assertTrue(TableFieldEquivalence.postgresUnboundedNumber(
            field("NUMERIC", FieldType.NUMBER, 131089, 0), DBType.PostgreSql));
        assertTrue(TableFieldEquivalence.sameDatabaseType(
            field("NUMERIC", FieldType.NUMBER, 131089, 0), field("NUMERIC", FieldType.NUMBER, 0, 0), DBType.PostgreSql));
        assertFalse(TableFieldEquivalence.postgresUnboundedNumber(
            field("NUMERIC", FieldType.NUMBER, 24, 2), DBType.PostgreSql));
    }

    @Test
    void unboundedBinaryIgnoresLengthWhileBoundedBinaryComparesIt() {
        // SimpleTableField.setColumnType 会剥掉 "(MAX)" 并把长度哨兵化为 -1，
        // 原始 JDBC TYPE_NAME 文本需要一个不解析类型的载体来承载。
        com.centit.support.database.metadata.TableField maxBinary =
            new com.centit.support.database.metadata.TableField() {
                @Override public String getPropertyName() { return null; }
                @Override public String getFieldType() { return FieldType.BYTE_ARRAY; }
                @Override public Class<?> getJavaType() { return byte[].class; }
                @Override public String getColumnName() { return "DATA"; }
                @Override public String getColumnType() { return "VARBINARY(MAX)"; }
                @Override public String getFieldLabelName() { return null; }
                @Override public String getColumnComment() { return null; }
                @Override public boolean isMandatory() { return false; }
                @Override public boolean isPrimaryKey() { return false; }
                @Override public boolean isLazyFetch() { return false; }
                @Override public Integer getMaxLength() { return null; }
                @Override public Integer getScale() { return null; }
                @Override public String getDefaultValue() { return null; }
            };
        assertFalse(TableFieldEquivalence.boundedBinary(maxBinary, DBType.SqlServer));
        // SqlServer 用 Integer.MAX_VALUE 表示 MAX 的 JDBC 哨兵同样视为无界。
        assertFalse(TableFieldEquivalence.boundedBinary(
            field("VARBINARY", FieldType.BYTE_ARRAY, Integer.MAX_VALUE, null), DBType.SqlServer));
        // 两种无界形态（"(MAX)" 文本与 JDBC MAX_VALUE 哨兵）互相等价，不比较长度。
        assertTrue(TableFieldEquivalence.sameDatabaseType(
            maxBinary, field("VARBINARY", FieldType.BYTE_ARRAY, Integer.MAX_VALUE, 0), DBType.SqlServer));
        // 有界形态必须比较长度。
        assertFalse(TableFieldEquivalence.sameDatabaseType(
            field("VARBINARY(16)", FieldType.BYTE_ARRAY, 16, 0),
            field("VARBINARY(8000)", FieldType.BYTE_ARRAY, 8000, 0), DBType.SqlServer));
        assertTrue(TableFieldEquivalence.sameDatabaseType(
            field("VARBINARY(16)", FieldType.BYTE_ARRAY, 16, 0),
            field("VARBINARY(16)", FieldType.BYTE_ARRAY, 16, 0), DBType.SqlServer));
    }

    @Test
    void differentPhysicalTypesAreNeverEquivalent() {
        assertFalse(TableFieldEquivalence.sameDatabaseType(
            field("INT", FieldType.INTEGER, 10, 0), field("BIGINT", FieldType.LONG, 19, 0), DBType.MySql));
        // 定长类型不比较长度：INTEGER 的驱动报告长度差异不构成差异。
        assertTrue(TableFieldEquivalence.sameDatabaseType(
            field("INT", FieldType.INTEGER, 10, 0), field("INT", FieldType.INTEGER, 11, 0), DBType.MySql));
    }
}
