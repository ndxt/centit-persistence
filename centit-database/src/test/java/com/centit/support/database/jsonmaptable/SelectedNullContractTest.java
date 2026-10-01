package com.centit.support.database.jsonmaptable;

import com.alibaba.fastjson2.JSONObject;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.utils.FieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

class SelectedNullContractTest {
    @Test
    void selectedSqlNullsRemainDeclaredAndAreNotCoercedToDefaultValues() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:selectedNulls", "sa", "")) {
            TableField[] fields = { field("nullableText", FieldType.STRING),
                    field("nullableFlag", FieldType.BOOLEAN), field("nullableNumber", FieldType.INTEGER),
                    field("nullableDate", FieldType.DATETIME), field("nullableJson", FieldType.JSON_OBJECT),
                    field("zero", FieldType.INTEGER), field("flag", FieldType.BOOLEAN), field("empty", FieldType.STRING) };
            JSONObject row = GeneralJsonObjectDao.findObjectsBySql(connection,
                    "select cast(null as varchar), cast(null as boolean), cast(null as integer), "
                            + "cast(null as timestamp), cast(null as varchar), 0, false, ''",
                    new Object[0], fields).getJSONObject(0);
            for (String name : List.of("nullableText", "nullableFlag", "nullableNumber", "nullableDate", "nullableJson")) {
                Assertions.assertTrue(row.containsKey(name), name);
                Assertions.assertNull(row.get(name));
            }
            Assertions.assertEquals(0, row.getIntValue("zero"));
            Assertions.assertFalse(row.getBooleanValue("flag"));
            Assertions.assertEquals("", row.getString("empty"));
        }
    }

    @Test
    void partialSelectionDoesNotInventUnselectedOrUnmappedFields() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:partialNulls", "sa", "")) {
            TableField[] fields = { field("selected", FieldType.STRING), null };
            JSONObject row = GeneralJsonObjectDao.findObjectsBySql(connection,
                    "select cast(null as varchar), cast(null as varchar)", new Object[0], fields).getJSONObject(0);
            Assertions.assertEquals(1, row.size());
            Assertions.assertTrue(row.containsKey("selected"));
            Assertions.assertNull(row.get("selected"));
            Assertions.assertFalse(row.containsKey("unselected"));
        }
    }

    private SimpleTableField field(String property, String type) {
        SimpleTableField field = new SimpleTableField();
        field.setColumnName(property);
        field.setPropertyName(property);
        field.setFieldType(type);
        return field;
    }
}
