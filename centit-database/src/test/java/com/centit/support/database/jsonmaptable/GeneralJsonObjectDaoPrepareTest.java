package com.centit.support.database.jsonmaptable;

import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.FieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

class GeneralJsonObjectDaoPrepareTest {

    private static SimpleTableField column(String property, String fieldType) {
        SimpleTableField field = new SimpleTableField();
        field.setPropertyName(property);
        field.setColumnName(property);
        field.setFieldType(fieldType);
        return field;
    }

    private static SimpleTableInfo table(SimpleTableField... fields) {
        SimpleTableInfo table = new SimpleTableInfo("PREPARE_TEST");
        for (SimpleTableField field : fields) table.addColumn(field);
        return table;
    }

    @Test
    void normalizesValuesByFieldTypeForJdbcWrites() {
        SimpleTableInfo tableInfo = table(
            column("AT_DATE", FieldType.DATE), column("AT_TIME", FieldType.TIMESTAMP),
            column("COUNT", FieldType.INTEGER), column("AMOUNT", FieldType.NUMBER),
            column("PRICE", FieldType.FLOAT), column("LABEL", FieldType.STRING),
            column("ENABLED", FieldType.BOOLEAN), column("PAYLOAD", FieldType.BYTE_ARRAY));
        Map<String, Object> row = new HashMap<>();
        row.put("AT_DATE", "2026-11-07");
        row.put("AT_TIME", "2026-11-07 10:30:00");
        row.put("COUNT", "7");
        row.put("AMOUNT", "123.450");
        row.put("PRICE", "2.5");
        row.put("LABEL", 100);
        row.put("ENABLED", Boolean.TRUE);
        row.put("PAYLOAD", "0102");

        Map<String, Object> prepared = GeneralJsonObjectDao.prepareObjectForSave(tableInfo, row);

        Assertions.assertSame(row, prepared);
        Assertions.assertEquals(java.util.Date.class, prepared.get("AT_DATE").getClass());
        Assertions.assertEquals(Timestamp.class, prepared.get("AT_TIME").getClass());
        Assertions.assertEquals(7L, prepared.get("COUNT"));
        Assertions.assertEquals(new BigDecimal("123.450"), prepared.get("AMOUNT"));
        Assertions.assertEquals(2.5, ((Number) prepared.get("PRICE")).doubleValue());
        Assertions.assertEquals("100", prepared.get("LABEL"));
        Assertions.assertEquals(BooleanBaseOpt.ONE_CHAR_TRUE, prepared.get("ENABLED"));
        // 字符串经 castObjectToBytes 按其字符字节转换（'0'→0x30），不做 hex 解码
        Assertions.assertArrayEquals("0102".getBytes(), (byte[]) prepared.get("PAYLOAD"));
    }

    @Test
    void keepsNullsAndUnknownTypesUntouchedAndPreservesMissingProperties() {
        SimpleTableInfo tableInfo = table(
            column("AMOUNT", FieldType.NUMBER), column("META", FieldType.JSON_OBJECT));
        Map<String, Object> row = new HashMap<>();
        row.put("AMOUNT", null);
        row.put("META", "{\"k\":1}");

        Map<String, Object> prepared = GeneralJsonObjectDao.prepareObjectForSave(tableInfo, row);

        Assertions.assertNull(prepared.get("AMOUNT"));
        // 未识别为需转换类型的字段保持原对象，不做任何改写
        Assertions.assertEquals("{\"k\":1}", prepared.get("META"));
        Assertions.assertFalse(prepared.containsKey("ABSENT"));
    }

    @Test
    void booleanFalseMapsToOneCharFalse() {
        SimpleTableInfo tableInfo = table(column("ENABLED", FieldType.BOOLEAN));
        Map<String, Object> row = new HashMap<>();
        row.put("ENABLED", "no");

        GeneralJsonObjectDao.prepareObjectForSave(tableInfo, row);

        Assertions.assertEquals(BooleanBaseOpt.ONE_CHAR_FALSE, row.get("ENABLED"));
    }
}
