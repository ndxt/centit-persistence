package com.centit.support.database.utils;

import com.centit.support.database.ddl.GeneralDDLOperations;
import com.centit.support.database.ddl.H2DDLOperations;
import com.centit.support.database.ddl.MySqlDDLOperations;
import com.centit.support.database.metadata.JdbcMetadata;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DDLUtilsRegressionTest {
    private SimpleTableInfo table(String type, String defaultValue) {
        SimpleTableInfo table = new SimpleTableInfo("T_ALTER");
        SimpleTableField field = new SimpleTableField();
        field.setColumnName("VALUE_COL");
        field.setColumnType(type);
        field.setMaxLength(16);
        field.setDefaultValue(defaultValue);
        table.addColumn(field);
        return table;
    }

    @Test
    void appliesDefaultOnlyChangeThroughDdlUtils() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:default_alter", "sa", "");
             Statement statement = conn.createStatement()) {
            H2DDLOperations ddl = new H2DDLOperations();
            statement.execute(ddl.makeCreateTableSql(table("VARCHAR", "'old'")));
            List<String> sqls = DDLUtils.makeAlterTableSqlList(table("VARCHAR", "'new'"),
                table("VARCHAR", "'old'"), DBType.H2, ddl);
            assertEquals(1, sqls.size());
            statement.execute(sqls.get(0));
            statement.execute("insert into T_ALTER default values");
            try (ResultSet rs = statement.executeQuery("select VALUE_COL from T_ALTER")) {
                assertTrue(rs.next());
                assertEquals("new", rs.getString(1));
            }
        }
    }

    @Test
    void changesTypeNativelyWithoutTemporaryColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:type_alter", "sa", "");
             Statement statement = conn.createStatement()) {
            H2DDLOperations ddl = new H2DDLOperations();
            SimpleTableInfo before = table("VARCHAR", null);
            statement.execute(ddl.makeCreateTableSql(before));
            statement.execute("insert into T_ALTER values('123')");
            List<String> sqls = DDLUtils.makeAlterTableSqlList(table("INTEGER", null), before, DBType.H2, ddl);
            for (String sql : sqls) {
                assertFalse(sql.toLowerCase().contains("drop column"));
                assertFalse(sql.toLowerCase().contains("rename"));
                statement.execute(sql);
            }
            try (ResultSet rs = statement.executeQuery("select VALUE_COL from T_ALTER")) {
                assertTrue(rs.next());
                assertEquals(123, rs.getInt(1));
            }
            JdbcMetadata metadata = new JdbcMetadata();
            metadata.setDBConfig(conn);
            assertEquals(1, metadata.getTableMetadata("T_ALTER").getColumns().size());
        }
    }

    @Test
    void rejectsIdentityToggleAndUnimplementedDialects() {
        SimpleTableInfo identity = table("BIGINT", null);
        identity.getColumns().get(0).setFieldType(FieldType.IDENTITY);
        assertThrows(UnsupportedOperationException.class, () -> DDLUtils.makeAlterTableSqlList(
            identity, table("BIGINT", null), DBType.H2, new H2DDLOperations()));
        assertThrows(UnsupportedOperationException.class, () -> new MySqlDDLOperations().makeCreateSequenceSql("SEQ"));
        assertThrows(RuntimeException.class, () -> GeneralDDLOperations.createDDLOperations(DBType.ClickHouse));
        assertThrows(UnsupportedOperationException.class, () -> FieldType.mapToDatabaseType(FieldType.IDENTITY, DBType.GBase));
        SimpleTableInfo primaryKey = table("BIGINT", null);
        primaryKey.getColumns().get(0).setPrimaryKey(true);
        assertThrows(UnsupportedOperationException.class, () -> DDLUtils.makeAlterTableSqlList(
            primaryKey, table("BIGINT", null), DBType.H2, new H2DDLOperations()));
    }

    @Test
    void ignoresIdentityImplicitNotNullWhenComparingWithMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:identity_compare", "sa", "");
             Statement statement = conn.createStatement()) {
            SimpleTableInfo design = table("BIGINT", null);
            design.getColumns().get(0).setFieldType(FieldType.IDENTITY);
            H2DDLOperations ddl = new H2DDLOperations();
            statement.execute(ddl.makeCreateTableSql(design));
            JdbcMetadata metadata = new JdbcMetadata();
            metadata.setDBConfig(conn);
            assertTrue(DDLUtils.makeAlterTableSqlList(design, metadata.getTableMetadata("T_ALTER"),
                DBType.H2, ddl).isEmpty());
        }
    }

    @Test
    void parserDoesNotTreatUniqueOrForeignKeysAsPrimaryKeys() {
        SimpleTableInfo table = GeneralDDLOperations.parseDDL("CREATE TABLE T_PARSE ("
            + "ID INTEGER NOT NULL, CODE VARCHAR(16), PARENT_ID INTEGER, "
            + "CONSTRAINT PK_PARSE PRIMARY KEY(ID), CONSTRAINT U_PARSE UNIQUE(CODE), "
            + "CONSTRAINT FK_PARSE FOREIGN KEY(PARENT_ID) REFERENCES PARENT(ID))");
        assertTrue(table.findFieldByColumn("ID").isPrimaryKey());
        assertTrue(table.findFieldByColumn("ID").isMandatory());
        assertFalse(table.findFieldByColumn("CODE").isPrimaryKey());
        assertFalse(table.findFieldByColumn("PARENT_ID").isPrimaryKey());
        SimpleTableInfo composite = GeneralDDLOperations.parseDDL("CREATE TABLE T (A INTEGER, B INTEGER, "
            + "FOREIGN KEY(A,B) REFERENCES P(PA,PB), PRIMARY KEY(B,A))");
        assertEquals(2, composite.getColumns().size());
        assertEquals("B", composite.getPkFields().get(0).getColumnName());
    }

    @Test
    void preservesIdentityWhenParsingPrecisionAndOrdersCompositePrimaryKey() {
        SimpleTableField identity = new SimpleTableField();
        identity.setColumnName("ID");
        identity.setColumnType(FieldType.mapToOracleColumnType(FieldType.IDENTITY));
        identity.mapToMetadata();
        assertEquals(FieldType.IDENTITY, identity.getFieldType());
        assertEquals(Long.class, identity.getJavaType());
        SimpleTableField h2Identity = new SimpleTableField();
        h2Identity.setColumnType(FieldType.mapToDatabaseType(FieldType.IDENTITY, DBType.H2));
        assertEquals("bigint", h2Identity.getColumnType());
        assertEquals(FieldType.IDENTITY, h2Identity.getFieldType());
        SimpleTableField sqlServerIdentity = new SimpleTableField();
        sqlServerIdentity.setColumnType(FieldType.mapToDatabaseType(FieldType.IDENTITY, DBType.SqlServer));
        assertEquals("bigint", sqlServerIdentity.getColumnType());
        SimpleTableInfo table = table("INTEGER", null);
        SimpleTableField second = table.getColumns().get(0);
        second.setPrimaryKey(true);
        second.setPrimaryKeyOrder(2);
        identity.setPrimaryKey(true);
        identity.setPrimaryKeyOrder(1);
        table.addColumn(identity);
        assertEquals("ID", table.getPkFields().get(0).getColumnName());
    }
}
