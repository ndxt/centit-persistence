package com.centit.support.database.metadata;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;

class JdbcBaselineReaderTest {

    @Test
    void normalizesJdbcTableKindsToStandardWords() {
        Assertions.assertEquals("TABLE", JdbcBaselineReader.tableType("TABLE"));
        Assertions.assertEquals("TABLE", JdbcBaselineReader.tableType("base table"));
        Assertions.assertEquals("VIEW", JdbcBaselineReader.tableType("View"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> JdbcBaselineReader.tableType("SYSTEM TABLE"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> JdbcBaselineReader.tableType("T"));
    }

    @Test
    void capturesCompositePrimaryKeysInSequenceOrder() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:baseline_keys");
             Statement statement = connection.createStatement()) {
            statement.execute("create table EXAMPLE (B int, A int, LABEL varchar(20), primary key (B, A))");
            JSONArray keys = JdbcBaselineReader.primaryKeys(connection, connection.getCatalog(), "PUBLIC", "EXAMPLE");
            Assertions.assertEquals(List.of("B", "A"),
                    keys.toJavaList(JSONObject.class).stream().map(row -> row.getString("columnName")).toList());
            Assertions.assertEquals(1, keys.getJSONObject(0).getIntValue("sequence"));
            Assertions.assertEquals(2, keys.getJSONObject(1).getIntValue("sequence"));
        }
    }

    @Test
    void capturesPlainIndexesAndSkipsPrimaryKeyBackingIndexes() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:baseline_indexes");
             Statement statement = connection.createStatement()) {
            statement.execute("create table EXAMPLE (ID int primary key, LABEL varchar(20), AGE int)");
            statement.execute("create index IX_LABEL on EXAMPLE(LABEL)");
            statement.execute("create unique index UQ_AGE on EXAMPLE(AGE)");
            JSONArray indexes = JdbcBaselineReader.indexes(connection, connection.getCatalog(), "PUBLIC", "EXAMPLE",
                    JdbcBaselineReader.primaryKeys(connection, connection.getCatalog(), "PUBLIC", "EXAMPLE"));
            List<JSONObject> names = indexes.toJavaList(JSONObject.class);
            Assertions.assertEquals(2, names.size());
            Assertions.assertEquals("IX_LABEL", names.get(0).getString("indexName"));
            Assertions.assertEquals("NORMAL", names.get(0).getString("indexType"));
            Assertions.assertEquals(List.of("LABEL"), names.get(0).getList("indexFields", String.class));
            Assertions.assertEquals("UQ_AGE", names.get(1).getString("indexName"));
            Assertions.assertEquals("UNIQUE", names.get(1).getString("indexType"));
        }
    }

    @Test
    void rejectsDescendingOrExpressionIndexesInsteadOfCapturingThemLossily() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:baseline_lossy");
             Statement statement = connection.createStatement()) {
            statement.execute("create table EXAMPLE (ID int primary key, LABEL varchar(20))");
            statement.execute("create index IX_DESC on EXAMPLE(LABEL desc)");
            SQLException error = Assertions.assertThrows(SQLException.class, () ->
                    JdbcBaselineReader.indexes(connection, connection.getCatalog(), "PUBLIC", "EXAMPLE",
                            JdbcBaselineReader.primaryKeys(connection, connection.getCatalog(), "PUBLIC", "EXAMPLE")));
            Assertions.assertTrue(error.getMessage().contains("cannot be captured"), error.getMessage());
        }
    }

    @Test
    void assertsAbsenceThroughQuerySemantics() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:baseline_absent");
             Statement statement = connection.createStatement()) {
            statement.execute("create table PRESENT(ID int)");
            Assertions.assertDoesNotThrow(() -> JdbcBaselineReader.assertTableAbsent(connection, "PUBLIC", "MISSING"));
            SQLException error = Assertions.assertThrows(SQLException.class,
                    () -> JdbcBaselineReader.assertTableAbsent(connection, "PUBLIC", "PRESENT"));
            Assertions.assertTrue(error.getMessage().contains("JDBC metadata is incomplete"), error.getMessage());
        }
    }

    @Test
    void readsViewDefinitionAndExtractsTheQuery() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:baseline_views");
             Statement statement = connection.createStatement()) {
            statement.execute("create table BASE(ID int, LABEL varchar(20))");
            statement.execute("create view V_SAMPLE as select ID, LABEL from BASE where ID > 0");
            Assertions.assertEquals("VIEW", JdbcBaselineReader.tableType(connection, "PUBLIC", "V_SAMPLE"));
            Assertions.assertNull(JdbcBaselineReader.tableType(connection, "PUBLIC", "MISSING"));
            String definition = JdbcBaselineReader.viewSql(connection, connection.getCatalog(), "PUBLIC", "V_SAMPLE");
            Assertions.assertTrue(definition.toLowerCase().contains("from"), definition);
        }
    }

    @Test
    void viewQueryExtractsOnlyTopLevelAs() throws Exception {
        Assertions.assertEquals("select 1",
                JdbcBaselineReader.viewQuery("create view X as select 1"));
        Assertions.assertEquals("select 1",
                JdbcBaselineReader.viewQuery("create view X as select 1;"));
        Assertions.assertEquals("select ' as ' from dual",
                JdbcBaselineReader.viewQuery("create view X as select ' as ' from dual"));
        Assertions.assertEquals("select /* as */ 1 from dual",
                JdbcBaselineReader.viewQuery("create view X as select /* as */ 1 from dual"));
        Assertions.assertEquals("select 1 from dual",
                JdbcBaselineReader.viewQuery("select 1 from dual"));
        Assertions.assertEquals("select (1 + 2) as total from dual",
                JdbcBaselineReader.viewQuery("create view X as select (1 + 2) as total from dual"));
        Assertions.assertThrows(SQLException.class,
                () -> JdbcBaselineReader.viewQuery("create index IX on EXAMPLE(ID)"));
    }
}
