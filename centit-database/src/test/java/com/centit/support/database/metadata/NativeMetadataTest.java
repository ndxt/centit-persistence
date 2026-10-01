package com.centit.support.database.metadata;

import com.centit.support.database.utils.FieldType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NativeMetadataTest {

    @Test
    void oracleReadsIdentityDefaultsCommentsAndRealForeignKeyColumns() throws Exception {
        try (Connection connection = open("oracle12")) {
            createOracleCatalog(connection, true);
            OracleMetadata metadata = new OracleMetadata();
            metadata.setDBConfig(withProductVersion(connection, "Oracle", 12));
            metadata.setDBSchema("APP");

            SimpleTableInfo table = metadata.getTableMetadata("ACCOUNT");
            assertEquals("APP", table.getSchema());
            assertEquals("Account table", table.getTableComment());
            assertEquals(2, table.getColumns().size());
            assertEquals(FieldType.IDENTITY, table.findFieldByColumn("ID").getFieldType());
            assertEquals("NUMBER", table.findFieldByColumn("ID").getColumnType());
            assertNull(table.findFieldByColumn("ID").getDefaultValue());
            assertEquals("Identifier", table.findFieldByColumn("ID").getColumnComment());
            assertEquals(FieldType.NUMBER, table.findFieldByColumn("CODE").getFieldType());
            assertEquals("APP.ACCOUNT_SEQ.NEXTVAL", table.findFieldByColumn("CODE").getDefaultValue());
            assertEquals(2, table.findFieldByColumn("ID").getPrimaryKeyOrder());
            assertEquals(1, table.findFieldByColumn("CODE").getPrimaryKeyOrder());
            assertEquals(2, table.getReferences().size());
            assertEquals("ACCOUNT_ID", table.findReference("FK_ACCOUNT").getReferenceColumns().get("ID"));
            assertEquals("ACCOUNT_CODE", table.findReference("FK_ACCOUNT").getReferenceColumns().get("CODE"));
            assertEquals("SECOND_CODE", table.findReference("FK_ACCOUNT_CODE").getReferenceColumns().get("CODE"));
            assertEquals(1, metadata.listTables(true, new String[]{"ACCOUNT"}).size());
            assertNull(metadata.getTableMetadata("MISSING"));
        }
    }

    @Test
    void oracle11AndOtherProductsDoNotReadNewIdentityCatalogColumns() throws Exception {
        for (String product : new String[]{"Oracle", "DM DBMS"}) {
            try (Connection connection = open("oldOracle" + product.hashCode())) {
                // The old catalog deliberately has no IDENTITY_COLUMN, so an unsupported query would fail.
                createOracleCatalog(connection, false);
                OracleMetadata metadata = new OracleMetadata();
                metadata.setDBConfig(withProductVersion(connection, product, "Oracle".equals(product) ? 11 : 19));
                metadata.setDBSchema("APP");
                SimpleTableInfo table = metadata.getTableMetadata("ACCOUNT");
                assertEquals(FieldType.NUMBER, table.findFieldByColumn("ID").getFieldType());
                assertEquals("APP.ISEQ.NEXTVAL", table.findFieldByColumn("ID").getDefaultValue());
                assertEquals("APP.ACCOUNT_SEQ.NEXTVAL", table.findFieldByColumn("CODE").getDefaultValue());
            }
        }
    }

    @Test
    void sqlServerReadsViewsAndAliasesWithoutDuplicatingColumns() throws Exception {
        try (Connection connection = open("sqlServer")) {
            createSqlServerCatalog(connection);
            SqlSvrMetadata metadata = new SqlSvrMetadata();
            metadata.setDBConfig(connection);
            metadata.setDBSchema("APP");

            SimpleTableInfo table = metadata.getTableMetadata("ACCOUNT");
            assertEquals("APP", table.getSchema());
            assertEquals("Account table", table.getTableComment());
            assertEquals(3, table.getColumns().size());
            assertEquals(FieldType.IDENTITY, table.findFieldByColumn("ID").getFieldType());
            assertNull(table.findFieldByColumn("ID").getDefaultValue());
            assertTrue(table.findFieldByColumn("ID").isMandatory());
            assertEquals("Identifier", table.findFieldByColumn("ID").getColumnComment());
            assertEquals("CODE_TYPE", table.findFieldByColumn("CODE").getColumnType());
            assertEquals(FieldType.LONG, table.findFieldByColumn("CODE").getFieldType());
            assertEquals("NEXT VALUE FOR APP.ACCOUNT_SEQ", table.findFieldByColumn("CODE").getDefaultValue());
            assertEquals(20, table.findFieldByColumn("NAME").getMaxLength());
            assertEquals(2, table.findFieldByColumn("ID").getPrimaryKeyOrder());
            assertEquals(1, table.findFieldByColumn("CODE").getPrimaryKeyOrder());
            assertEquals(2, table.getReferences().size());
            assertEquals("ACCOUNT_ID", table.findReference("FK_ACCOUNT").getReferenceColumns().get("ID"));
            assertEquals("ACCOUNT_CODE", table.findReference("FK_ACCOUNT").getReferenceColumns().get("CODE"));
            assertEquals("OTHER_ACCOUNT_ID", table.findReference("FK_SECOND").getReferenceColumns().get("ID"));

            SimpleTableInfo view = metadata.getTableMetadata("ACCOUNT_VIEW");
            assertEquals("V", view.getTableType());
            assertEquals(2, view.getColumns().size());
            assertEquals(FieldType.STRING, view.findFieldByColumn("DETAILS").getFieldType());
            assertEquals(-1, view.findFieldByColumn("DETAILS").getMaxLength());
            assertEquals(2, metadata.listTables(true, new String[]{"ACCOUNT", "ACCOUNT_VIEW"}).size());
            assertNull(metadata.getTableMetadata("MISSING"));
        }
    }

    @Test
    void db2DistinguishesIdentityAndPrimaryKeyFromUniqueKey() throws Exception {
        try (Connection connection = open("db2")) {
            execute(connection,
                "CREATE SCHEMA APP",
                "CREATE TABLE APP.ACCOUNT (ID BIGINT GENERATED BY DEFAULT AS IDENTITY, CODE BIGINT, NAME VARCHAR(20), " +
                    "CONSTRAINT PK_ACCOUNT PRIMARY KEY (ID), CONSTRAINT U_ACCOUNT_CODE UNIQUE (CODE))",
                "CREATE TABLE APP.CHILD (ACCOUNT_ID BIGINT, ACCOUNT_CODE BIGINT, " +
                    "CONSTRAINT FK_ACCOUNT FOREIGN KEY (ACCOUNT_ID) REFERENCES APP.ACCOUNT(ID), " +
                    "CONSTRAINT FK_ACCOUNT_CODE FOREIGN KEY (ACCOUNT_CODE) REFERENCES APP.ACCOUNT(CODE))",
                "CREATE SCHEMA SYSIBM",
                "CREATE TABLE SYSIBM.SYSTABLES (NAME VARCHAR, TYPE VARCHAR, REMARKS VARCHAR, CREATOR VARCHAR)",
                "INSERT INTO SYSIBM.SYSTABLES VALUES ('ACCOUNT','T','Account table','APP'), ('ACCOUNT','T','Other table','OTHER')",
                "CREATE TABLE SYSIBM.SYSCOLUMNS (NAME VARCHAR, COLTYPE VARCHAR, LENGTH INT, SCALE INT, NULLS VARCHAR, " +
                    "REMARKS VARCHAR, DEFAULT VARCHAR, DEFAULTVALUE VARCHAR, TBNAME VARCHAR, TBCREATOR VARCHAR, COLNO INT)",
                "INSERT INTO SYSIBM.SYSCOLUMNS VALUES " +
                    "('ID','BIGINT',19,0,'N','Identifier','J','','ACCOUNT','APP',1)," +
                    "('CODE','BIGINT',19,0,'Y','','4','42','ACCOUNT','APP',2)," +
                    "('NAME','VARCHAR',20,0,'Y','','1','O''Reilly','ACCOUNT','APP',3)," +
                    "('ALIEN','INTEGER',10,0,'Y','','N','','ACCOUNT','OTHER',1)",
                "CREATE TABLE SYSIBM.SYSTABCONST (TBCREATOR VARCHAR, TBNAME VARCHAR, CONSTNAME VARCHAR, TYPE VARCHAR)",
                "INSERT INTO SYSIBM.SYSTABCONST VALUES ('APP','ACCOUNT','PK_ACCOUNT','P'), ('APP','ACCOUNT','U_ACCOUNT_CODE','U')",
                "CREATE TABLE SYSIBM.SYSKEYCOLUSE (TBCREATOR VARCHAR, TBNAME VARCHAR, CONSTNAME VARCHAR, COLNAME VARCHAR, COLSEQ INT)",
                "INSERT INTO SYSIBM.SYSKEYCOLUSE VALUES ('APP','ACCOUNT','PK_ACCOUNT','ID',1), ('APP','ACCOUNT','U_ACCOUNT_CODE','CODE',1)"
            );
            DB2Metadata metadata = new DB2Metadata();
            metadata.setDBConfig(connection);
            metadata.setDBSchema("app");

            SimpleTableInfo table = metadata.getTableMetadata("ACCOUNT");
            assertEquals("APP", table.getSchema());
            assertEquals("Account table", table.getTableComment());
            assertEquals(3, table.getColumns().size());
            assertEquals(FieldType.IDENTITY, table.findFieldByColumn("ID").getFieldType());
            assertEquals("BIGINT", table.findFieldByColumn("ID").getColumnType());
            assertNull(table.findFieldByColumn("ID").getDefaultValue());
            assertEquals("Identifier", table.findFieldByColumn("ID").getColumnComment());
            assertTrue(table.findFieldByColumn("ID").isPrimaryKey());
            assertEquals(1, table.findFieldByColumn("ID").getPrimaryKeyOrder());
            assertFalse(table.findFieldByColumn("CODE").isPrimaryKey());
            assertEquals("42", table.findFieldByColumn("CODE").getDefaultValue());
            assertEquals("'O''Reilly'", table.findFieldByColumn("NAME").getDefaultValue());
            assertEquals(2, table.getReferences().size());
            assertEquals("ACCOUNT_ID", table.findReference("FK_ACCOUNT").getReferenceColumns().get("ID"));
            assertEquals("ACCOUNT_CODE", table.findReference("FK_ACCOUNT_CODE").getReferenceColumns().get("CODE"));
            assertEquals(1, metadata.listTables(true, new String[]{"ACCOUNT"}).size());
            assertNull(metadata.getTableMetadata("MISSING"));
        }
    }

    @Test
    void queryFailuresAreReportedInsteadOfReturningPartialMetadata() {
        Connection failing = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
            new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                throw new SQLException("Catalog access denied");
            });
        for (DatabaseMetadata metadata : List.of(new OracleMetadata(), new DB2Metadata(), new SqlSvrMetadata())) {
            metadata.setDBConfig(failing);
            metadata.setDBSchema("APP");
            IllegalStateException tableError = assertThrows(IllegalStateException.class,
                () -> metadata.getTableMetadata("ACCOUNT"));
            assertInstanceOf(SQLException.class, tableError.getCause());
            assertThrows(IllegalStateException.class, () -> metadata.listTables(true, null));
        }
    }

    private Connection open(String name) throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:native_metadata_" + name +
            ";MODE=MSSQLServer;NON_KEYWORDS=DEFAULT,PRECISION,POSITION,VALUE");
    }

    private void execute(Connection connection, String... sqls) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String sql : sqls) {
                statement.execute(sql);
            }
        }
    }

    private Connection withProductVersion(Connection connection, String product, int majorVersion) throws SQLException {
        DatabaseMetaData delegate = connection.getMetaData();
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(DatabaseMetaData.class.getClassLoader(),
            new Class<?>[]{DatabaseMetaData.class}, (proxy, method, args) -> {
                if (method.getName().equals("getDatabaseProductName")) {
                    return product;
                }
                if (method.getName().equals("getDatabaseMajorVersion")) {
                    return majorVersion;
                }
                try {
                    return method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
            (proxy, method, args) -> {
                if (method.getName().equals("getMetaData")) {
                    return metadata;
                }
                try {
                    return method.invoke(connection, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });
    }

    private void createOracleCatalog(Connection connection, boolean identityColumn) throws SQLException {
        execute(connection,
            "CREATE TABLE ALL_TAB_COMMENTS (TABLE_NAME VARCHAR, TABLE_TYPE VARCHAR, COMMENTS VARCHAR, OWNER VARCHAR)",
            "INSERT INTO ALL_TAB_COMMENTS VALUES ('ACCOUNT','TABLE','Account table','APP'), ('ACCOUNT','TABLE','Other table','OTHER')",
            "CREATE TABLE ALL_TAB_COLUMNS (COLUMN_NAME VARCHAR, DATA_TYPE VARCHAR, DATA_LENGTH INT, DATA_PRECISION INT, " +
                "DATA_SCALE INT, NULLABLE VARCHAR, DATA_DEFAULT VARCHAR, OWNER VARCHAR, TABLE_NAME VARCHAR, " +
                "COLUMN_ID INT, CHAR_USED VARCHAR, CHAR_LENGTH INT" + (identityColumn ? ", IDENTITY_COLUMN VARCHAR" : "") + ")",
            "INSERT INTO ALL_TAB_COLUMNS VALUES ('ID','NUMBER',22,18,0,'N','APP.ISEQ.NEXTVAL','APP','ACCOUNT',1,NULL,0" +
                (identityColumn ? ",'YES'" : "") + ")," +
                "('CODE','NUMBER',22,18,0,'N',' APP.ACCOUNT_SEQ.NEXTVAL ','APP','ACCOUNT',2,NULL,0" +
                (identityColumn ? ",'NO'" : "") + ")," +
                "('ALIEN','NUMBER',22,18,0,'Y',NULL,'OTHER','ACCOUNT',1,NULL,0" + (identityColumn ? ",'NO'" : "") + ")",
            "CREATE TABLE ALL_COL_COMMENTS (COLUMN_NAME VARCHAR, TABLE_NAME VARCHAR, OWNER VARCHAR, COMMENTS VARCHAR)",
            "INSERT INTO ALL_COL_COMMENTS VALUES ('ID','ACCOUNT','APP','Identifier'), ('ID','ACCOUNT','OTHER','Other identifier')",
            "CREATE TABLE ALL_CONSTRAINTS (OWNER VARCHAR, TABLE_NAME VARCHAR, CONSTRAINT_NAME VARCHAR, " +
                "CONSTRAINT_TYPE VARCHAR, R_OWNER VARCHAR, R_CONSTRAINT_NAME VARCHAR)",
            "INSERT INTO ALL_CONSTRAINTS VALUES ('APP','ACCOUNT','PK_ACCOUNT','P',NULL,NULL)," +
                "('APP','ACCOUNT','U_ACCOUNT_CODE','U',NULL,NULL)," +
                "('APP','CHILD','FK_ACCOUNT','R','APP','PK_ACCOUNT')," +
                "('APP','CHILD','FK_ACCOUNT_CODE','R','APP','U_ACCOUNT_CODE')," +
                "('OTHER','CHILD','FK_OTHER','R','APP','PK_ACCOUNT')",
            "CREATE TABLE ALL_CONS_COLUMNS (OWNER VARCHAR, CONSTRAINT_NAME VARCHAR, COLUMN_NAME VARCHAR, POSITION INT)",
            "INSERT INTO ALL_CONS_COLUMNS VALUES ('APP','PK_ACCOUNT','CODE',1), ('APP','PK_ACCOUNT','ID',2)," +
                "('APP','U_ACCOUNT_CODE','CODE',1), ('APP','FK_ACCOUNT','ACCOUNT_CODE',1), ('APP','FK_ACCOUNT','ACCOUNT_ID',2)," +
                "('APP','FK_ACCOUNT_CODE','SECOND_CODE',1), ('OTHER','FK_OTHER','ALIEN_ID',1)"
        );
    }

    private void createSqlServerCatalog(Connection connection) throws SQLException {
        execute(connection,
            "CREATE SCHEMA SYS",
            "CREATE TABLE SYS.SCHEMAS (SCHEMA_ID INT, NAME VARCHAR)",
            "INSERT INTO SYS.SCHEMAS VALUES (1,'APP'), (2,'OTHER')",
            "CREATE TABLE SYS.OBJECTS (NAME VARCHAR, TYPE VARCHAR, OBJECT_ID INT, SCHEMA_ID INT, IS_MS_SHIPPED INT)",
            "INSERT INTO SYS.OBJECTS VALUES ('ACCOUNT','U',1,1,0), ('ACCOUNT_VIEW','V',2,1,0), " +
                "('ACCOUNT','U',3,2,0), ('CHILD','U',4,1,0), ('CHILD','U',5,2,0)",
            "CREATE TABLE SYS.EXTENDED_PROPERTIES (CLASS INT, MAJOR_ID INT, MINOR_ID INT, NAME VARCHAR, VALUE VARCHAR)",
            "INSERT INTO SYS.EXTENDED_PROPERTIES VALUES (1,1,0,'MS_Description','Account table'), " +
                "(1,1,1,'MS_Description','Identifier'), (2,1,1,'MS_Description','Wrong class comment')",
            "CREATE TABLE SYS.TYPES (NAME VARCHAR, USER_TYPE_ID INT, SYSTEM_TYPE_ID INT, IS_USER_DEFINED INT)",
            "INSERT INTO SYS.TYPES VALUES ('bigint',127,127,0), ('nvarchar',231,231,0), " +
                "('CODE_TYPE',1004,127,1), ('OTHER_ALIAS',1005,127,1)",
            "CREATE TABLE SYS.COLUMNS (NAME VARCHAR, USER_TYPE_ID INT, SYSTEM_TYPE_ID INT, MAX_LENGTH INT, PRECISION INT, " +
                "SCALE INT, IS_NULLABLE INT, IS_IDENTITY INT, DEFAULT_OBJECT_ID INT, OBJECT_ID INT, COLUMN_ID INT)",
            "INSERT INTO SYS.COLUMNS VALUES ('ID',127,127,8,19,0,0,1,0,1,1), ('CODE',1004,127,8,19,0,1,0,500,1,2)," +
                "('NAME',231,231,40,0,0,1,0,0,1,3), ('VIEW_ID',127,127,8,19,0,1,0,0,2,1)," +
                "('DETAILS',231,231,-1,0,0,1,0,0,2,2)," +
                "('ACCOUNT_ID',127,127,8,19,0,1,0,0,4,1), ('ACCOUNT_CODE',127,127,8,19,0,1,0,0,4,2)," +
                "('OTHER_ACCOUNT_ID',127,127,8,19,0,1,0,0,4,3), ('ALIEN_ID',127,127,8,19,0,1,0,0,5,1)",
            "CREATE TABLE SYS.DEFAULT_CONSTRAINTS (OBJECT_ID INT, DEFINITION VARCHAR)",
            "INSERT INTO SYS.DEFAULT_CONSTRAINTS VALUES (500,'NEXT VALUE FOR APP.ACCOUNT_SEQ')",
            "CREATE TABLE SYS.KEY_CONSTRAINTS (NAME VARCHAR, PARENT_OBJECT_ID INT, UNIQUE_INDEX_ID INT, TYPE VARCHAR)",
            "INSERT INTO SYS.KEY_CONSTRAINTS VALUES ('PK_ACCOUNT',1,1,'PK')",
            "CREATE TABLE SYS.INDEX_COLUMNS (OBJECT_ID INT, INDEX_ID INT, COLUMN_ID INT, KEY_ORDINAL INT)",
            "INSERT INTO SYS.INDEX_COLUMNS VALUES (1,1,1,2), (1,1,2,1)",
            "CREATE TABLE SYS.FOREIGN_KEYS (NAME VARCHAR, OBJECT_ID INT, PARENT_OBJECT_ID INT, REFERENCED_OBJECT_ID INT)",
            "INSERT INTO SYS.FOREIGN_KEYS VALUES ('FK_ACCOUNT',100,4,1), ('FK_SECOND',101,4,1), ('FK_OTHER',102,5,1)",
            "CREATE TABLE SYS.FOREIGN_KEY_COLUMNS (CONSTRAINT_OBJECT_ID INT, PARENT_OBJECT_ID INT, PARENT_COLUMN_ID INT, " +
                "REFERENCED_OBJECT_ID INT, REFERENCED_COLUMN_ID INT, CONSTRAINT_COLUMN_ID INT)",
            "INSERT INTO SYS.FOREIGN_KEY_COLUMNS VALUES (100,4,1,1,1,1), (100,4,2,1,2,2), (101,4,3,1,1,1), (102,5,1,1,1,1)"
        );
    }
}
