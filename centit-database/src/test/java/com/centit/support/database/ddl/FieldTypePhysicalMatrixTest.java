package com.centit.support.database.ddl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.centit.support.database.metadata.JdbcMetadata;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** New, isolated physical databases: never opens the running service's H2 files. */
class FieldTypePhysicalMatrixTest {
    private final H2DDLOperations ddl = new H2DDLOperations();

    static Stream<String> types() {
        return Stream.of(FieldType.STRING, FieldType.INTEGER, FieldType.NUMBER, FieldType.FLOAT,
            FieldType.MONEY, FieldType.DOUBLE, FieldType.LONG, FieldType.BOOLEAN, FieldType.DATE,
            FieldType.DATETIME, FieldType.TIMESTAMP, FieldType.FILE_ID, FieldType.ENUM_NAME,
            FieldType.TEXT, FieldType.BYTE_ARRAY, FieldType.FILE, FieldType.JSON_OBJECT);
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:type_matrix_" + UUID.randomUUID(), "sa", "");
    }

    private static SimpleTableField field(String name, String type) {
        SimpleTableField field = new SimpleTableField();
        field.setColumnName(name);
        field.setColumnType(FieldType.mapToDatabaseType(type, DBType.H2));
        field.setFieldType(type);
        if (FieldType.STRING.equals(type)) {
            field.setMaxLength(128);
        } else if (FieldType.NUMBER.equals(type)) {
            field.setMaxLength(12);
            field.setScale(3);
        }
        field.setFieldLabelName("type " + type);
        return field;
    }

    private static SimpleTableInfo table(String name, SimpleTableField... fields) {
        SimpleTableInfo table = new SimpleTableInfo(name);
        for (SimpleTableField field : fields) {
            table.addColumn(field);
        }
        return table;
    }

    private static SimpleTableInfo reverse(Connection connection, String name) {
        JdbcMetadata metadata = new JdbcMetadata();
        metadata.setDBConfig(connection);
        return metadata.getTableMetadata(name);
    }

    private static String physicalSemantic(String type) {
        return switch (type) {
            case FieldType.FILE_ID, FieldType.ENUM_NAME, FieldType.BOOLEAN -> FieldType.STRING;
            case FieldType.MONEY -> FieldType.NUMBER;
            case FieldType.FLOAT -> FieldType.DOUBLE; // H2 FLOAT has 53-bit precision.
            case FieldType.DATETIME -> FieldType.TIMESTAMP;
            case FieldType.FILE -> FieldType.BYTE_ARRAY;
            case FieldType.JSON_OBJECT -> FieldType.TEXT;
            default -> type;
        };
    }

    private static Object value(String type) {
        return switch (type) {
            case FieldType.INTEGER -> 123456;
            case FieldType.LONG -> 9223372036854775806L;
            case FieldType.NUMBER -> new BigDecimal("123456789.123");
            case FieldType.MONEY -> new BigDecimal("12345678901234567890.1234");
            case FieldType.FLOAT, FieldType.DOUBLE -> 12345.125d;
            case FieldType.BOOLEAN -> "T";
            case FieldType.DATE -> java.sql.Date.valueOf("2026-10-02");
            case FieldType.DATETIME, FieldType.TIMESTAMP -> Timestamp.valueOf("2026-10-02 12:34:56.123456");
            case FieldType.FILE_ID -> "file-" + "x".repeat(59);
            case FieldType.ENUM_NAME -> "ENUM_" + "x".repeat(59);
            case FieldType.TEXT -> "large text ' 中文 ".repeat(400);
            case FieldType.JSON_OBJECT -> "{\"value\":\"" + "中文x".repeat(400) + "\"}";
            case FieldType.BYTE_ARRAY, FieldType.FILE -> new byte[]{0, 1, 2, 3, -1, 127};
            default -> "string ' 中文 value";
        };
    }

    private static String defaultValue(String type) {
        return switch (type) {
            case FieldType.INTEGER, FieldType.LONG -> "7";
            case FieldType.NUMBER, FieldType.MONEY, FieldType.FLOAT, FieldType.DOUBLE -> "7.125";
            case FieldType.DATE -> "DATE '2026-10-02'";
            case FieldType.DATETIME, FieldType.TIMESTAMP -> "TIMESTAMP '2026-10-02 12:34:56.123456'";
            case FieldType.BYTE_ARRAY, FieldType.FILE -> "X'010203'";
            case FieldType.BOOLEAN -> "'F'";
            default -> "'seed'";
        };
    }

    private static Object defaultData(String type) {
        return switch (type) {
            case FieldType.INTEGER -> 7;
            case FieldType.LONG -> 7L;
            case FieldType.NUMBER, FieldType.MONEY -> new BigDecimal("7.125");
            case FieldType.FLOAT, FieldType.DOUBLE -> 7.125d;
            case FieldType.DATE -> java.sql.Date.valueOf("2026-10-02");
            case FieldType.DATETIME, FieldType.TIMESTAMP -> Timestamp.valueOf("2026-10-02 12:34:56.123456");
            case FieldType.BYTE_ARRAY, FieldType.FILE -> new byte[]{1, 2, 3};
            case FieldType.BOOLEAN -> "F";
            default -> "seed";
        };
    }

    private static void assertValue(ResultSet result, int index, Object expected) throws SQLException {
        if (expected instanceof byte[] bytes) {
            assertArrayEquals(bytes, result.getBytes(index));
        } else if (expected instanceof BigDecimal decimal) {
            assertEquals(0, decimal.compareTo(result.getBigDecimal(index)));
        } else if (expected instanceof java.sql.Date date) {
            assertEquals(date, result.getDate(index));
        } else if (expected instanceof Timestamp timestamp) {
            assertEquals(timestamp, result.getTimestamp(index));
        } else if (expected instanceof Integer integer) {
            assertEquals(integer.intValue(), result.getInt(index));
        } else if (expected instanceof Long longValue) {
            assertEquals(longValue.longValue(), result.getLong(index));
        } else if (expected instanceof Double doubleValue) {
            assertEquals(doubleValue.doubleValue(), result.getDouble(index));
        } else {
            assertEquals(expected, result.getString(index));
        }
    }

    @ParameterizedTest(name = "{0}: physical create, metadata, constraints, data and reverse recreate")
    @MethodSource("types")
    void createsReadsAndRecreatesEveryPersistedType(String type) throws Exception {
        SimpleTableField id = field("ID", FieldType.LONG);
        id.setPrimaryKey(true);
        SimpleTableField nullable = field("VAL", type);
        SimpleTableField required = field("REQUIRED_VALUE", type);
        required.setMandatory(true);
        required.setDefaultValue(defaultValue(type));
        String sql = ddl.makeCreateTableSql(table("TYPE_MATRIX", id, nullable, required));
        System.out.println("TYPE-MATRIX draft=" + type + " create=" + sql);
        try (Connection conn = connection(); Statement statement = conn.createStatement()) {
            statement.execute(sql);
            // LOB columns are deliberately not indexed: H2 does not support LOB indexes.
            if (!List.of(FieldType.TEXT, FieldType.JSON_OBJECT, FieldType.BYTE_ARRAY, FieldType.FILE).contains(type)) {
                statement.execute("create index IX_TYPE_VALUE on TYPE_MATRIX(VAL)");
                try (ResultSet index = conn.getMetaData().getIndexInfo(null, "PUBLIC", "TYPE_MATRIX", false, false)) {
                    boolean found = false;
                    while (index.next()) {
                        found |= "IX_TYPE_VALUE".equals(index.getString("INDEX_NAME"));
                    }
                    assertTrue(found);
                }
            }
            SimpleTableInfo back = reverse(conn, "TYPE_MATRIX");
            SimpleTableField backValue = back.findFieldByColumn("VAL");
            System.out.println("TYPE-MATRIX reverse=" + type + " physical=" + backValue.getColumnType()
                + " length=" + backValue.getMaxLength() + " scale=" + backValue.getScale()
                + " semantic=" + backValue.getFieldType());
            assertEquals(physicalSemantic(type), backValue.getFieldType());
            assertFalse(backValue.isMandatory());
            assertTrue(back.findFieldByColumn("REQUIRED_VALUE").isMandatory());
            assertNotNull(back.findFieldByColumn("REQUIRED_VALUE").getDefaultValue());
            assertTrue(back.findFieldByColumn("ID").isPrimaryKey());
            assertTrue(back.findFieldByColumn("ID").isMandatory());
            if (FieldType.STRING.equals(type)) {
                assertEquals(128, backValue.getMaxLength());
            } else if (FieldType.FILE_ID.equals(type) || FieldType.ENUM_NAME.equals(type)) {
                assertEquals(64, backValue.getMaxLength());
            } else if (FieldType.NUMBER.equals(type)) {
                assertEquals(12, backValue.getMaxLength());
                assertEquals(3, backValue.getScale());
            } else if (FieldType.MONEY.equals(type)) {
                assertEquals(24, backValue.getMaxLength());
                assertEquals(4, backValue.getScale());
            }
            Object expected = value(type);
            try (PreparedStatement insert = conn.prepareStatement("insert into TYPE_MATRIX(ID, VAL) values (?, ?)")) {
                insert.setLong(1, 1);
                insert.setObject(2, expected);
                assertEquals(1, insert.executeUpdate());
            }
            statement.execute("insert into TYPE_MATRIX(ID, VAL) values (2, null)");
            assertThrows(SQLException.class, () -> statement.execute("insert into TYPE_MATRIX(ID) values (1)"));
            assertThrows(SQLException.class, () -> statement.execute("insert into TYPE_MATRIX(ID, REQUIRED_VALUE) values (3, null)"));
            try (ResultSet result = statement.executeQuery("select VAL, REQUIRED_VALUE from TYPE_MATRIX where ID=1")) {
                assertTrue(result.next());
                assertValue(result, 1, expected);
                assertValue(result, 2, defaultData(type));
            }
            try (ResultSet result = statement.executeQuery("select VAL from TYPE_MATRIX where ID=2")) {
                assertTrue(result.next());
                assertNull(result.getObject(1));
            }
            back.setTableName("TYPE_MATRIX_COPY");
            statement.execute(ddl.makeCreateTableSql(back));
            try (PreparedStatement insert = conn.prepareStatement("insert into TYPE_MATRIX_COPY(ID, VAL) values (?, ?)")) {
                insert.setLong(1, 1);
                insert.setObject(2, expected);
                assertEquals(1, insert.executeUpdate());
            }
            try (ResultSet result = statement.executeQuery("select VAL from TYPE_MATRIX_COPY")) {
                assertTrue(result.next());
                assertValue(result, 1, expected);
            }
        }
    }

    @Test
    void jdbcTypeJavaProjectionsPreserveLongAndExactDecimalData() throws Exception {
        SimpleTableField id = field("ID", FieldType.LONG);
        id.setPrimaryKey(true);
        SimpleTableField decimal = field("DEC", FieldType.NUMBER);
        decimal.setMaxLength(24);
        decimal.setScale(4);
        SimpleTableField numeric = field("NUM", FieldType.NUMBER);
        numeric.setColumnType("NUMERIC(24,4)");
        BigDecimal amount = new BigDecimal("12345678901234567890.1234");
        try (Connection conn = connection(); Statement statement = conn.createStatement()) {
            statement.execute(ddl.makeCreateTableSql(table("JAVA_NUMERIC_MATRIX", id, decimal, numeric)));
            try (PreparedStatement insert = conn.prepareStatement("insert into JAVA_NUMERIC_MATRIX values (?, ?, ?)")) {
                insert.setLong(1, Long.MAX_VALUE);
                insert.setBigDecimal(2, amount);
                insert.setBigDecimal(3, amount);
                assertEquals(1, insert.executeUpdate());
            }
            SimpleTableInfo back = reverse(conn, "JAVA_NUMERIC_MATRIX");
            try (ResultSet result = statement.executeQuery("select * from JAVA_NUMERIC_MATRIX")) {
                assertTrue(result.next());
                System.out.println("JAVA-PROJECTION JDBC BIGINT=" + FieldType.mapToJavaType(Types.BIGINT)
                    + " NUMERIC=" + FieldType.mapToJavaType(Types.NUMERIC)
                    + " DECIMAL=" + FieldType.mapToJavaType(Types.DECIMAL)
                    + " metadata decimal=" + back.findFieldByColumn("DEC").getJavaType());
                assertAll("JDBC codes and metadata must preserve physical wide and exact numbers",
                    () -> assertEquals(Long.class, FieldType.mapToJavaType(Types.BIGINT)),
                    () -> assertEquals(BigDecimal.class, FieldType.mapToJavaType(Types.NUMERIC)),
                    () -> assertEquals(BigDecimal.class, FieldType.mapToJavaType(Types.DECIMAL)),
                    () -> assertEquals(FieldType.LONG, FieldType.mapToFieldType(Types.BIGINT)),
                    () -> assertEquals(FieldType.NUMBER, FieldType.mapToFieldType(Types.NUMERIC)),
                    () -> assertEquals(FieldType.NUMBER, FieldType.mapToFieldType(Types.DECIMAL)),
                    () -> assertEquals(Long.MAX_VALUE, result.getObject(1,
                        FieldType.mapToJavaType(result.getMetaData().getColumnType(1)))),
                    () -> assertEquals(amount, result.getObject(2,
                        FieldType.mapToJavaType(result.getMetaData().getColumnType(2)))),
                    () -> assertEquals(amount, result.getObject(3, FieldType.mapToJavaType(Types.DECIMAL))),
                    () -> assertEquals(BigDecimal.class, back.findFieldByColumn("DEC").getJavaType()),
                    () -> assertEquals(BigDecimal.class, back.findFieldByColumn("NUM").getJavaType()),
                    () -> assertEquals(amount, result.getObject(2, back.findFieldByColumn("DEC").getJavaType())));
            }
        }
    }

    @Test
    void logicalJavaProjectionsReadActualBinaryAndObjectData() throws Exception {
        SimpleTableField bytes = field("BIN", FieldType.BYTE_ARRAY);
        SimpleTableField file = field("FILE_VALUE", FieldType.FILE);
        SimpleTableField object = field("DOC", FieldType.JSON_OBJECT);
        byte[] binary = new byte[]{0, 1, 2, -1, 127};
        JSONObject document = JSONObject.of("text", "中文 ' JSON", "amount", new BigDecimal("123.4567"));
        try (Connection conn = connection(); Statement statement = conn.createStatement()) {
            statement.execute(ddl.makeCreateTableSql(table("JAVA_OBJECT_MATRIX", bytes, file, object)));
            try (PreparedStatement insert = conn.prepareStatement("insert into JAVA_OBJECT_MATRIX values (?, ?, ?)")) {
                insert.setBytes(1, binary);
                insert.setBytes(2, binary);
                insert.setString(3, document.toJSONString());
                assertEquals(1, insert.executeUpdate());
            }
            try (ResultSet result = statement.executeQuery("select * from JAVA_OBJECT_MATRIX")) {
                assertTrue(result.next());
                Class<?> objectType = object.getJavaType();
                // The existing ORM uses JSON.class as the marker for JSON.parse, rather than String projection.
                Object projectedObject = JSON.class.equals(objectType)
                    ? JSON.parse(result.getString(3)) : result.getObject(3, objectType);
                System.out.println("JAVA-PROJECTION logical bytes=" + bytes.getJavaType() + " file="
                    + file.getJavaType() + " object=" + objectType + " parsed=" + projectedObject.getClass());
                assertAll("logical binary and object mappings must read their physical payloads",
                    () -> assertEquals(byte[].class, bytes.getJavaType()),
                    () -> assertEquals(byte[].class, file.getJavaType()),
                    () -> assertEquals(JSON.class, objectType),
                    () -> assertEquals(JSON.class, FieldType.mapToJavaType("OBJECT")),
                    () -> assertArrayEquals(binary, (byte[]) result.getObject(1, bytes.getJavaType())),
                    () -> assertArrayEquals(binary, (byte[]) result.getObject(2, file.getJavaType())),
                    () -> assertEquals(document, projectedObject));
            }
        }
        SimpleTableField beanField = field("BIN", FieldType.BYTE_ARRAY);
        beanField.setObjectField(ProjectionBean.class.getDeclaredField("value"));
        assertEquals(String.class, beanField.getJavaType(), "explicit Java bean mapping remains authoritative");
    }

    private static class ProjectionBean {
        private String value;
    }

    @Test
    void variableBinaryLengthAndLobPhysicalDefinitionsSurviveReverseRecreate() throws Exception {
        SimpleTableField bytes = field("BIN", FieldType.BYTE_ARRAY);
        bytes.setColumnType("VARBINARY");
        bytes.setMaxLength(256);
        SimpleTableField text = field("TXT", FieldType.TEXT);
        SimpleTableField blob = field("LOB_BIN", FieldType.BYTE_ARRAY);
        try (Connection conn = connection(); Statement statement = conn.createStatement()) {
            statement.execute(ddl.makeCreateTableSql(table("NATIVE_MATRIX", bytes, text, blob)));
            SimpleTableInfo back = reverse(conn, "NATIVE_MATRIX");
            assertEquals(256, back.findFieldByColumn("BIN").getMaxLength());
            assertEquals(FieldType.BYTE_ARRAY, back.findFieldByColumn("BIN").getFieldType());
            assertEquals("CHARACTER LARGE OBJECT", back.findFieldByColumn("TXT").getColumnType());
            assertEquals(FieldType.TEXT, back.findFieldByColumn("TXT").getFieldType());
            assertEquals("BINARY LARGE OBJECT", back.findFieldByColumn("LOB_BIN").getColumnType());
            assertEquals(FieldType.BYTE_ARRAY, back.findFieldByColumn("LOB_BIN").getFieldType());
            back.setTableName("NATIVE_MATRIX_COPY");
            statement.execute(ddl.makeCreateTableSql(back));
            SimpleTableInfo copy = reverse(conn, "NATIVE_MATRIX_COPY");
            assertEquals(256, copy.findFieldByColumn("BIN").getMaxLength());
            assertEquals(back.findFieldByColumn("TXT").getColumnType(), copy.findFieldByColumn("TXT").getColumnType());
            assertEquals(back.findFieldByColumn("LOB_BIN").getColumnType(), copy.findFieldByColumn("LOB_BIN").getColumnType());
            byte[] smallBinary = new byte[256];
            byte[] largeBinary = new byte[65537];
            smallBinary[255] = 42;
            largeBinary[65536] = 42;
            String largeText = "中文长文本".repeat(20000);
            try (PreparedStatement insert = conn.prepareStatement("insert into NATIVE_MATRIX_COPY values (?, ?, ?)")) {
                insert.setBytes(1, smallBinary);
                insert.setString(2, largeText);
                insert.setBytes(3, largeBinary);
                assertEquals(1, insert.executeUpdate());
                insert.setBytes(1, new byte[257]);
                assertThrows(SQLException.class, insert::executeUpdate);
            }
            try (ResultSet result = statement.executeQuery("select * from NATIVE_MATRIX_COPY")) {
                assertTrue(result.next());
                assertArrayEquals(smallBinary, result.getBytes(1));
                assertEquals(largeText, result.getString(2));
                assertArrayEquals(largeBinary, result.getBytes(3));
                assertFalse(result.next());
            }
        }
    }

    @Test
    void binaryAndTemporalDialectMappingsUseDataTypesThatPreserveTheirData() {
        for (String type : List.of(FieldType.BYTE_ARRAY, FieldType.FILE)) {
            SimpleTableField binary = new SimpleTableField();
            binary.setColumnName("BIN");
            binary.setColumnType(FieldType.mapToDatabaseType(type, DBType.PostgreSql));
            binary.setFieldType(type);
            assertEquals("create table T (  BIN bytea)", new PostgreSqlDDLOperations().makeCreateTableSql(table("T", binary)));
            assertEquals("BLOB", FieldType.mapToDatabaseType(type, DBType.MySql));
            assertEquals(byte[].class, FieldType.mapToJavaType("BYTEA"));
            assertEquals(FieldType.BYTE_ARRAY, FieldType.mapToFieldType("BYTEA"));
        }
        SimpleTableField timestamp = new SimpleTableField();
        timestamp.setColumnName("TS");
        timestamp.setColumnType(FieldType.mapToDatabaseType(FieldType.TIMESTAMP, DBType.SqlServer));
        timestamp.setFieldType(FieldType.TIMESTAMP);
        assertEquals("create table T (  TS datetime2)", new SqlSvrDDLOperations().makeCreateTableSql(table("T", timestamp)));
        assertEquals(Timestamp.class, FieldType.mapToJavaType("DATETIME2"));
        assertEquals(FieldType.TIMESTAMP, FieldType.mapToFieldType("DATETIME2"));
        assertEquals(FieldType.TEXT, FieldType.mapToFieldType("LONGTEXT"));
    }

    @Test
    void relatedDialectMappingsDoNotNarrowLongOrTimestampAndUnsupportedMappingsFail() {
        assertEquals(18, FieldType.getAllTypeMap().size());
        assertTrue(FieldType.getAllTypeMap().containsKey(FieldType.ENUM_NAME));
        assertFalse(FieldType.getAllTypeMap().containsKey(FieldType.OBJECT_LIST));
        assertFalse(FieldType.getAllTypeMap().containsKey(FieldType.VOID));
        assertEquals("自增字段", FieldType.getAllTypeMap().get(FieldType.IDENTITY));
        assertAll("Oracle long capacity and PostgreSQL money precision",
            () -> assertEquals("19", FieldType.LONG_NUMBER_DIGITS),
            () -> assertEquals("number(19,0)", FieldType.mapToDatabaseType(FieldType.LONG, DBType.Oracle)),
            () -> assertEquals("DECIMAL(24,4)", FieldType.mapToDatabaseType(FieldType.MONEY, DBType.PostgreSql)),
            () -> assertTrue(new BigDecimal(Long.toString(Long.MIN_VALUE)).precision() <= Integer.parseInt(FieldType.LONG_NUMBER_DIGITS)),
            () -> assertTrue(new BigDecimal(Long.toString(Long.MAX_VALUE)).precision() <= Integer.parseInt(FieldType.LONG_NUMBER_DIGITS)));
        SimpleTableField postgresMoney = new SimpleTableField();
        postgresMoney.setColumnName("AMOUNT");
        postgresMoney.setColumnType(FieldType.mapToDatabaseType(FieldType.MONEY, DBType.PostgreSql));
        postgresMoney.setFieldType(FieldType.MONEY);
        assertEquals("create table T (  AMOUNT DECIMAL(24,4))", new PostgreSqlDDLOperations().makeCreateTableSql(table("T", postgresMoney)));
        assertEquals("BIGINT", FieldType.mapToDatabaseType(FieldType.LONG, DBType.DB2));
        assertEquals("TimeStamp", FieldType.mapToDatabaseType(FieldType.DATETIME, DBType.DB2));
        assertEquals("Date", FieldType.mapToDatabaseType(FieldType.DATE, DBType.DB2));
        assertEquals("Decimal(24,4)", FieldType.mapToDatabaseType(FieldType.MONEY, DBType.ClickHouse));
        assertEquals("DateTime", FieldType.mapToDatabaseType(FieldType.DATETIME, DBType.ClickHouse));
        assertEquals("DateTime64(6)", FieldType.mapToDatabaseType(FieldType.TIMESTAMP, DBType.ClickHouse));
        assertThrows(UnsupportedOperationException.class, () -> FieldType.mapToDatabaseType(FieldType.STRING, DBType.Unknown));
        assertThrows(UnsupportedOperationException.class, () -> FieldType.mapToDatabaseType(FieldType.STRING, DBType.Access));
        assertEquals("datetime year to second", FieldType.mapToDatabaseType(FieldType.DATETIME, DBType.GBase));
        assertEquals("datetime year to fraction(5)", FieldType.mapToDatabaseType(FieldType.TIMESTAMP, DBType.GBase));
        SimpleTableField gbaseTimestamp = new SimpleTableField();
        gbaseTimestamp.setColumnName("TS");
        gbaseTimestamp.setColumnType(FieldType.mapToDatabaseType(FieldType.TIMESTAMP, DBType.GBase));
        gbaseTimestamp.setFieldType(FieldType.TIMESTAMP);
        assertEquals("create table T (  TS datetime year to fraction(5))", GeneralDDLOperations.createDDLOperations(DBType.GBase).makeCreateTableSql(table("T", gbaseTimestamp)));
        assertThrows(UnsupportedOperationException.class, () -> FieldType.mapToDatabaseType(FieldType.IDENTITY, DBType.ClickHouse));
        assertThrows(UnsupportedOperationException.class, () -> FieldType.mapToDatabaseType(FieldType.IDENTITY, DBType.GBase));
    }

    @Test
    void identityWithoutPrimaryKeyStillGeneratesAndRejectsNullAndKeepsSequenceAfterAlter() throws Exception {
        SimpleTableField identity = field("ID", FieldType.IDENTITY);
        SimpleTableField code = field("CODE", FieldType.STRING);
        code.setPrimaryKey(true);
        try (Connection conn = connection(); Statement statement = conn.createStatement()) {
            statement.execute(ddl.makeCreateTableSql(table("IDENTITY_MATRIX", identity, code)));
            statement.execute("insert into IDENTITY_MATRIX(CODE) values ('a'), ('b')");
            assertThrows(SQLException.class, () -> statement.execute("insert into IDENTITY_MATRIX(ID, CODE) values (null, 'c')"));
            SimpleTableField back = reverse(conn, "IDENTITY_MATRIX").findFieldByColumn("ID");
            assertEquals(FieldType.IDENTITY, back.getFieldType());
            assertTrue(back.isMandatory());
            assertFalse(back.isPrimaryKey());
            assertNull(back.getDefaultValue());
            statement.execute("alter table IDENTITY_MATRIX alter column ID set data type BIGINT");
            statement.execute("insert into IDENTITY_MATRIX(CODE) values ('c')");
            try (ResultSet result = statement.executeQuery("select ID from IDENTITY_MATRIX order by ID")) {
                for (int expected = 1; expected <= 3; expected++) {
                    assertTrue(result.next());
                    assertEquals(expected, result.getLong(1));
                }
            }
            assertEquals(FieldType.IDENTITY, reverse(conn, "IDENTITY_MATRIX").findFieldByColumn("ID").getFieldType());
        }
        identity.setDefaultValue("1");
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeCreateTableSql(table("T", identity, code)));
    }
}
