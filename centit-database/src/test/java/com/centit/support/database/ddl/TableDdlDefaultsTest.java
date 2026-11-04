package com.centit.support.database.ddl;

import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.FieldType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableDdlDefaultsTest {

    @Test
    void postgresReadbackCastsAndTransactionTimeDefaultsAreEquivalent() {
        assertEquals(
            TableDdlDefaults.normalizeDefaultValue("'DRAFT'", FieldType.STRING, DBType.PostgreSql, false),
            TableDdlDefaults.normalizeDefaultValue("'DRAFT'::character varying", FieldType.STRING, DBType.PostgreSql, true));
        assertEquals(
            TableDdlDefaults.normalizeDefaultValue("CURRENT_TIMESTAMP", FieldType.DATETIME, DBType.PostgreSql, false),
            TableDdlDefaults.normalizeDefaultValue("now()", FieldType.DATETIME, DBType.PostgreSql, true));
        assertNotEquals(
            TableDdlDefaults.normalizeDefaultValue("' a '", FieldType.STRING, DBType.PostgreSql, false),
            TableDdlDefaults.normalizeDefaultValue("'a'::text", FieldType.STRING, DBType.PostgreSql, true));
        assertNotEquals(
            TableDdlDefaults.normalizeDefaultValue("clock_timestamp()", FieldType.DATETIME, DBType.PostgreSql, false),
            TableDdlDefaults.normalizeDefaultValue("now()", FieldType.DATETIME, DBType.PostgreSql, true));
    }

    private String h2(String value) {
        return TableDdlDefaults.normalizeDefaultValue(value, FieldType.STRING, DBType.H2, true);
    }

    @Test
    void distinguishesAbsentEmptyLiteralAndExpressions() {
        assertEquals("", h2(null));
        assertNotEquals(h2(null), h2("''"));
        assertNotEquals(h2("'CURRENT_TIMESTAMP'"), h2("CURRENT_TIMESTAMP"));
        assertNotEquals(h2("'NULL'"), h2("NULL"));
        assertNotEquals(h2("' '"), h2("''"));
        assertNotEquals(h2("'A' || 'B'"), h2("'A'' || ''B'"));
    }

    @Test
    void standaloneNullKeywordConvergesWithoutRewritingLiteralsOrOtherExpressions() {
        for (DBType dialect : new DBType[]{DBType.H2, DBType.MySql, DBType.Oracle}) {
            assertEquals("expression:NULL",
                TableDdlDefaults.normalizeDefaultValue("null\n", FieldType.DATE, dialect, false));
            assertEquals("expression:NULL",
                TableDdlDefaults.normalizeDefaultValue(" NuLl ", FieldType.DATE, dialect, true));
        }
        assertEquals("literal:null", h2("'null'"));
        assertEquals("literal:NULL", h2("'NULL'"));
        assertNotEquals(h2("'null'"), h2("'NULL'"));
        assertEquals("literal:null", TableDdlDefaults.normalizeDefaultValue("null", FieldType.STRING, DBType.MySql, true));
        assertEquals("expression:(null)", h2("(null)"));
        assertEquals("expression:sysdate", h2("sysdate\n"));
        assertNotEquals(h2("sysdate"), h2("SYSDATE"));
    }

    @Test
    void decodesH2UnicodeAndQuotedEscapesWithoutLosingLiteralKind() {
        assertEquals(h2("'升'"), h2("U&'\\5347'"));
        assertEquals(h2("'O''Brien'"), h2("U&'O''Brien'"));
        assertEquals(h2("'C:\\files'"), h2("U&'C:\\\\files'"));
        assertNotEquals(h2("'升'"), h2("升"));
    }

    @Test
    void typedNumericConstantsConvergeButExpressionTextRemainsDistinct() {
        assertEquals(h2("0"), h2("'0'"));
        assertEquals(
            TableDdlDefaults.normalizeDefaultValue("'0.0'", FieldType.LONG, DBType.H2, false),
            TableDdlDefaults.normalizeDefaultValue("0", FieldType.LONG, DBType.H2, true));
        for (String type : new String[]{FieldType.NUMBER, FieldType.MONEY}) {
            assertEquals("number:12345678901234567890.1234",
                TableDdlDefaults.normalizeDefaultValue("'12345678901234567890.123400'", type, DBType.H2, false));
            assertEquals(
                TableDdlDefaults.normalizeDefaultValue("'0.00'", type, DBType.H2, false),
                TableDdlDefaults.normalizeDefaultValue("0", type, DBType.H2, true));
        }
        assertNotEquals(h2("CURRENT_TIMESTAMP"), h2("current_timestamp"));
        assertNotEquals(h2("(1 + 2)"), h2("(1+2)"));
    }

    @Test
    void mysqlBareCharacterDefaultsNeedBothPhysicalSourceAndDialect() {
        assertEquals("literal:", TableDdlDefaults.normalizeDefaultValue("", FieldType.STRING, DBType.MySql, true));
        assertEquals("literal: ", TableDdlDefaults.normalizeDefaultValue(" ", FieldType.STRING, DBType.MySql, true));
        assertEquals("literal:升", TableDdlDefaults.normalizeDefaultValue("升", FieldType.STRING, DBType.MySql, true));
        assertEquals("expression:升", TableDdlDefaults.normalizeDefaultValue("升", FieldType.STRING, DBType.MySql, false));
        assertEquals("expression:升", TableDdlDefaults.normalizeDefaultValue("升", FieldType.STRING, null, true));
        assertEquals("expression:(uuid())", TableDdlDefaults.normalizeDefaultValue("(uuid())", FieldType.STRING, DBType.MySql, true));
    }

    @Test
    void mysqlBooleanConstantsConvergeWithNativeNumericDefaults() {
        assertEquals(
            TableDdlDefaults.normalizeDefaultValue("1", FieldType.STRING, DBType.MySql, true),
            TableDdlDefaults.normalizeDefaultValue("true", FieldType.STRING, DBType.MySql, false));
        assertEquals("literal:true", TableDdlDefaults.normalizeDefaultValue("true", FieldType.STRING, DBType.MySql, true));
        assertEquals("expression:true", TableDdlDefaults.normalizeDefaultValue("true", FieldType.STRING, DBType.H2, false));
    }

    private SimpleTableField column(String name, String type, int length, int scale, String defaultValue) {
        SimpleTableField field = new SimpleTableField();
        field.setColumnName(name);
        field.setFieldType(FieldType.STRING);
        field.setColumnType(type);
        field.setMaxLength(length);
        field.setScale(scale);
        field.setDefaultValue(defaultValue);
        return field;
    }

    @Test
    void equivalentDefaultSqlKeepsLabelChangesAndDoesNotMutateEitherDefinition() {
        SimpleTableField oldColumn = column("UNIT", "VARCHAR", 16, 0, "U&'\\5347'");
        oldColumn.setFieldLabelName("旧名称");
        SimpleTableField newColumn = column("UNIT", "VARCHAR", 16, 0, "'升'");
        newColumn.setFieldLabelName("新名称");
        DDLOperations ddl = TableDdlDefaults.wrap(GeneralDDLOperations.createDDLOperations(DBType.H2), DBType.H2);
        assertEquals(List.of("comment on column SAMPLE.UNIT is '新名称'"),
            ddl.makeModifyColumnSqls("SAMPLE", oldColumn, newColumn));
        assertEquals("U&'\\5347'", oldColumn.getDefaultValue());
        assertEquals("旧名称", oldColumn.getFieldLabelName());
        assertEquals("'升'", newColumn.getDefaultValue());
        newColumn.setDefaultValue("'桶'");
        assertEquals(List.of("alter table SAMPLE alter column UNIT set default '桶'",
                "comment on column SAMPLE.UNIT is '新名称'"),
            ddl.makeModifyColumnSqls("SAMPLE", oldColumn, newColumn));
    }

    @Test
    void nullKeywordReadbackDoesNotRequestAnUnnecessaryDefaultAlteration() {
        SimpleTableField physical = column("UPDATED_AT", "DATE", 0, 0, "NULL");
        physical.setFieldType(FieldType.DATE);
        SimpleTableField requested = column("UPDATED_AT", "DATE", 0, 0, "null\n");
        requested.setFieldType(FieldType.DATE);
        DDLOperations ddl = TableDdlDefaults.wrap(GeneralDDLOperations.createDDLOperations(DBType.H2), DBType.H2);
        assertTrue(ddl.makeModifyColumnSqls("NULL_DEFAULT", physical, requested).isEmpty());
        assertEquals("NULL", physical.getDefaultValue());
        assertEquals("null\n", requested.getDefaultValue());
    }

    @Test
    void postgresUnboundedNumberPreservesBareNumericWithoutJdbcSentinelPrecision() {
        SimpleTableInfo table = new SimpleTableInfo("UNBOUNDED_TEST");
        SimpleTableField amount = new SimpleTableField();
        amount.setColumnName("AMOUNT");
        amount.setFieldType(FieldType.NUMBER);
        amount.setColumnType("NUMERIC");
        amount.setMaxLength(131089);
        amount.setScale(0);
        table.addColumn(amount);
        DDLOperations ddl = TableDdlDefaults.wrap(GeneralDDLOperations.createDDLOperations(DBType.PostgreSql), DBType.PostgreSql);
        String create = ddl.makeCreateTableSql(table, true);
        assertTrue(create.matches("(?is).*AMOUNT numeric(?:[,\\s)].*)?"), create);
        assertFalse(create.toLowerCase().contains("131089"), create);
        // JDBC 回读哨兵与设计侧无约束 numeric 等价，不应产生任何 modify 语句。
        SimpleTableField source = new SimpleTableField();
        source.setColumnName("AMOUNT");
        source.setFieldType(FieldType.NUMBER);
        source.setColumnType("numeric");
        source.setMaxLength(24);
        source.setScale(-127);
        assertTrue(ddl.makeModifyColumnSqls("UNBOUNDED_TEST", amount, source).isEmpty());
    }
}
