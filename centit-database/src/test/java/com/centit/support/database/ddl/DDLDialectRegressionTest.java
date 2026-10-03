package com.centit.support.database.ddl;

import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.DDLUtils;
import com.centit.support.database.utils.FieldType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DDLDialectRegressionTest {
    private static SimpleTableField column(String name, String type, int length, boolean mandatory, String defaultValue, String label) {
        SimpleTableField field = new SimpleTableField();
        field.setColumnName(name);
        field.setColumnType(type);
        field.setMaxLength(length);
        field.setMandatory(mandatory);
        field.setDefaultValue(defaultValue);
        field.setFieldLabelName(label);
        return field;
    }

    private static SimpleTableInfo table(String name, SimpleTableField... fields) {
        SimpleTableInfo table = new SimpleTableInfo(name);
        for (SimpleTableField field : fields) {
            table.addColumn(field);
        }
        return table;
    }

    @Test
    void mysqlModifyAndRenameRetainTheCompleteDefinition() {
        MySqlDDLOperations ddl = new MySqlDDLOperations();
        SimpleTableField oldField = column("C", "VARCHAR", 16, true, "'N'", "user's name");
        SimpleTableField newField = column("C", "VARCHAR", 32, true, "'N'", "user's name");
        String sql = ddl.makeModifyColumnSqls("T", oldField, newField).get(0);
        assertEquals("alter table T MODIFY COLUMN C VARCHAR(32) default 'N' not null comment 'user''s name'", sql);
        newField.setColumnName("D");
        assertEquals("alter table T CHANGE C D VARCHAR(32) default 'N' not null comment 'user''s name'",
            ddl.makeRenameColumnSql("T", "C", newField));
        oldField.setMandatory(false);
        oldField.setFieldLabelName(null);
        newField.setColumnName("C");
        newField.setMaxLength(16);
        assertEquals("alter table T MODIFY COLUMN C VARCHAR(16) default 'N' not null comment 'user''s name'",
            ddl.makeModifyColumnSqls("T", oldField, newField).get(0));
        assertEquals(List.of(), ddl.makeModifyColumnSqls("T", newField, newField));
    }

    @Test
    void mysqlColumnCommentsPreserveBackslashesQuotesAndWhitespace() {
        MySqlDDLOperations ddl = new MySqlDDLOperations();
        SimpleTableField oldField = column("C", "VARCHAR", 16, false, null, "old");
        SimpleTableField field = column("C", "VARCHAR", 32, false, null, " 路径 C:\\new\\path\\ '中文 ");
        String expectedComment = "comment ' 路径 C:\\\\new\\\\path\\\\ ''中文 '";
        assertTrue(ddl.makeCreateTableSql(table("T", field)).endsWith(expectedComment + ")"));
        assertTrue(ddl.makeAddColumnSql("T", field).endsWith(expectedComment));
        assertTrue(ddl.makeModifyColumnSqls("T", oldField, field).get(0).endsWith(expectedComment));
        assertTrue(ddl.makeRenameColumnSql("T", "OLD_C", field).endsWith(expectedComment));
    }

    @Test
    void oracleDefaultsPrecedeNotNullAndAreIncludedForAddedColumns() {
        OracleDDLOperations ddl = new OracleDDLOperations();
        SimpleTableField field = column("C", "VARCHAR2", 20, true, "'N'", null);
        assertEquals("create table T (  C VARCHAR2(20) default 'N' not null)", ddl.makeCreateTableSql(table("T", field)));
        assertEquals("alter table T add   C VARCHAR2(20) default 'N' not null", ddl.makeAddColumnSql("T", field));
        SimpleTableField changed = column("C", "VARCHAR2", 20, true, null, null);
        assertTrue(ddl.makeModifyColumnSqls("T", field, changed).get(0).contains("default null"));
    }

    @Test
    void sqlServerUsesNativeAddRenameAndNullableSyntaxWithoutLosingMax() {
        SqlSvrDDLOperations ddl = new SqlSvrDDLOperations();
        SimpleTableField oldField = column("C", "NVARCHAR", -1, false, null, null);
        SimpleTableField newField = column("C", "NVARCHAR", -1, true, null, null);
        assertEquals(List.of("alter table T ALTER COLUMN C NVARCHAR(MAX) not null"), ddl.makeModifyColumnSqls("T", oldField, newField));
        newField.setColumnName("D");
        assertEquals("exec sp_rename 'dbo.T.C','D','COLUMN'", ddl.makeRenameColumnSql("dbo.T", "C", newField));
        assertEquals("alter table T add   D NVARCHAR(MAX) not null", ddl.makeAddColumnSql("T", newField));
        assertEquals("create view V as select 1 as C", ddl.makeCreateViewSql("select 1 as C", "V"));
        oldField.setColumnType("VARBINARY");
        newField.setColumnType("VARBINARY");
        newField.setColumnName("C");
        assertTrue(ddl.makeModifyColumnSqls("T", oldField, newField).get(0).contains("VARBINARY(MAX)"));
    }

    @Test
    void sqlServerExplicitlyRejectsDefaultAndCommentChanges() {
        SqlSvrDDLOperations ddl = new SqlSvrDDLOperations();
        SimpleTableField oldField = column("C", "INT", 0, false, null, null);
        SimpleTableField newField = column("C", "INT", 0, false, "1", null);
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeModifyColumnSqls("T", oldField, newField));
        newField.setDefaultValue(null);
        newField.setFieldLabelName("label");
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeModifyColumnSqls("T", oldField, newField));
    }

    @Test
    void db2UsesCommentOnAndNativeDefaultChanges() {
        DB2DDLOperations ddl = new DB2DDLOperations();
        SimpleTableField oldField = column("C", "INTEGER", 0, false, null, null);
        SimpleTableField newField = column("C", "INTEGER", 0, false, "1", "user's name");
        assertEquals(List.of("alter table T alter column C set default 1", "comment on column T.C is 'user''s name'"),
            ddl.makeModifyColumnSqls("T", oldField, newField));
        assertEquals(List.of("COMMENT ON COLUMN T.C IS 'user''s name'"), ddl.makeTableColumnComments(table("T", newField), 1));
    }

    @Test
    void sqliteRejectsUnsupportedChangesAndRequiresASingleIdentityPrimaryKey() {
        SqliteDDLOperations ddl = new SqliteDDLOperations();
        SimpleTableField identity = column("ID", "INTEGER", 0, false, null, null);
        identity.setFieldType(FieldType.IDENTITY);
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeCreateTableSql(table("T", identity)));
        identity.setPrimaryKey(true);
        SimpleTableField otherKey = column("CODE", "INTEGER", 0, false, null, null);
        otherKey.setPrimaryKey(true);
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeCreateTableSql(table("T", identity, otherKey)));
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeAddColumnSql("T", identity));
        assertTrue(ddl.makeCreateTableSql(table("T", identity)).contains("primary key AUTOINCREMENT"));
        assertFalse(ddl.makeCreateTableSql(table("T", otherKey)).contains("AUTOINCREMENT"));
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeModifyColumnSqls("T", otherKey, otherKey));
        assertThrows(UnsupportedOperationException.class, () -> ddl.makeCreateSequenceSql("S"));
        assertEquals("create view V as select 1 as C", ddl.makeCreateViewSql("select 1 as C", "V"));
    }

    @Test
    void identityTypeChangesOnlyUsePhysicalTypesAndCannotToggleIdentity() {
        SimpleTableField oldField = column("ID", "INTEGER", 0, true, null, null);
        oldField.setFieldType(FieldType.IDENTITY);
        SimpleTableField newField = column("ID", "BIGINT", 0, true, null, null);
        newField.setFieldType(FieldType.IDENTITY);
        GeneralDDLOperations[] dialects = {new H2DDLOperations(), new DB2DDLOperations(), new PostgreSqlDDLOperations(), new SqlSvrDDLOperations()};
        for (GeneralDDLOperations ddl : dialects) {
            String sql = ddl.makeModifyColumnSqls("T", oldField, newField).get(0);
            assertTrue(sql.contains("BIGINT"));
            assertFalse(sql.toUpperCase().contains("IDENTITY"));
            assertFalse(sql.toUpperCase().contains("SERIAL"));
            newField.setFieldType(FieldType.LONG);
            assertThrows(UnsupportedOperationException.class, () -> ddl.makeModifyColumnSqls("T", oldField, newField));
            newField.setFieldType(FieldType.IDENTITY);
        }
        assertTrue(new PostgreSqlDDLOperations().makeCreateTableSql(table("T", newField)).contains("BIGSERIAL"));
        oldField.setColumnType("SMALLINT");
        assertTrue(new PostgreSqlDDLOperations().makeCreateTableSql(table("T", oldField)).contains("SMALLSERIAL"));
    }

    @Test
    void oracleRejectsIdentityOnOlderOracleConnections() {
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "getDatabaseProductName" -> "Oracle";
                case "getDatabaseMajorVersion" -> 11;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        Connection conn = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
            (proxy, method, args) -> {
                if ("getMetaData".equals(method.getName())) {
                    return metadata;
                }
                throw new UnsupportedOperationException(method.getName());
            });
        SimpleTableField identity = column("ID", "NUMBER", 18, true, null, null);
        identity.setFieldType(FieldType.IDENTITY);
        assertThrows(UnsupportedOperationException.class, () -> new OracleDDLOperations(conn).makeCreateTableSql(table("T", identity)));
    }

    @Test
    void h2ConvertsStringToMoneyWithoutDroppingExistingValues() throws Exception {
        SimpleTableField oldField = column("C", "VARCHAR", 20, false, null, "value");
        SimpleTableField newField = column("C", "DECIMAL", 10, false, null, "value");
        newField.setFieldType(FieldType.MONEY);
        newField.setScale(2);
        List<String> sqls = DDLUtils.makeAlterTableSqlList(table("T", newField), table("T", oldField), DBType.H2, null);
        assertEquals(1, sqls.size());
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:ddl_money", "sa", ""); var stmt = conn.createStatement()) {
            stmt.execute(new H2DDLOperations().makeCreateTableSql(table("T", oldField)));
            stmt.execute("insert into T values ('123.45')");
            for (String sql : sqls) {
                stmt.execute(sql);
            }
            try (var rs = stmt.executeQuery("select C from T")) {
                assertTrue(rs.next());
                assertEquals(new BigDecimal("123.45"), rs.getBigDecimal(1));
            }
        }
    }

    @Test
    void h2FailedTypeConversionLeavesTheOriginalColumnAndValue() throws Exception {
        SimpleTableField oldField = column("C", "VARCHAR", 20, false, null, null);
        SimpleTableField newField = column("C", "DECIMAL", 10, false, null, null);
        newField.setFieldType(FieldType.MONEY);
        newField.setScale(2);
        H2DDLOperations ddl = new H2DDLOperations();
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:ddl_failed_conversion", "sa", ""); var stmt = conn.createStatement()) {
            stmt.execute(ddl.makeCreateTableSql(table("T", oldField)));
            stmt.execute("insert into T values ('bad-value')");
            assertThrows(SQLException.class, () -> stmt.execute(ddl.makeModifyColumnSqls("T", oldField, newField).get(0)));
            try (var rs = stmt.executeQuery("select C from T")) {
                assertTrue(rs.next());
                assertEquals("bad-value", rs.getString(1));
                assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(1));
            }
        }
    }

    @Test
    void h2IdentityWidthChangesRetainSequenceAndCommentsHandleQuotesAndClearing() throws Exception {
        H2DDLOperations ddl = new H2DDLOperations();
        SimpleTableField oldField = column("ID", "INTEGER", 0, true, null, "user's key");
        oldField.setFieldType(FieldType.IDENTITY);
        SimpleTableField newField = column("ID", "BIGINT", 0, true, null, "user's key");
        newField.setFieldType(FieldType.IDENTITY);
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:ddl_identity_width", "sa", ""); var stmt = conn.createStatement()) {
            stmt.execute(ddl.makeCreateTableSql(table("T", oldField)));
            stmt.execute("insert into T default values");
            for (String sql : ddl.makeModifyColumnSqls("T", oldField, newField)) {
                stmt.execute(sql);
            }
            stmt.execute("insert into T default values");
            try (var rs = stmt.executeQuery("select ID from T order by ID")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
                assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(1));
            }
            oldField.setColumnType("BIGINT");
            newField.setFieldLabelName(null);
            for (String sql : ddl.makeModifyColumnSqls("T", oldField, newField)) {
                stmt.execute(sql);
            }
            try (var rs = conn.getMetaData().getColumns(null, null, "T", "ID")) {
                assertTrue(rs.next());
                assertEquals("YES", rs.getString("IS_AUTOINCREMENT"));
                assertTrue(rs.getString("REMARKS") == null || rs.getString("REMARKS").isEmpty());
            }
        }
    }
}
