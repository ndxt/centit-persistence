package com.centit.support.database.ddl;

import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.DDLUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H2 方言 DDL 真实执行验证：标准模式（未开启 MySQL 兼容）下
 * 建表、修改列（类型/可空/默认值/注释）与重命名列必须全部可执行。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class H2DDLOperationsTest {

    private static Connection conn;
    private final H2DDLOperations ddlOpt = new H2DDLOperations();

    @BeforeAll
    static void initDatabase() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:h2ddltest;DB_CLOSE_DELAY=-1", "sa", "");
    }

    @AfterAll
    static void closeDatabase() throws SQLException {
        conn.close();
    }

    private static SimpleTableField column(String name, String columnType, int maxLength,
            boolean mandatory, String defaultValue, String labelName) {
        SimpleTableField field = new SimpleTableField();
        field.setColumnName(name);
        field.setColumnType(columnType);
        field.setMaxLength(maxLength);
        field.setMandatory(mandatory);
        field.setDefaultValue(defaultValue);
        field.setFieldLabelName(labelName);
        return field;
    }

    private static SimpleTableInfo tableOf(String tableName, SimpleTableField... columns) {
        SimpleTableInfo tableInfo = new SimpleTableInfo();
        tableInfo.setTableName(tableName);
        for (SimpleTableField column : columns) {
            tableInfo.addColumn(column);
        }
        return tableInfo;
    }

    private static void execute(String sql) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    private static boolean columnExists(String tableName, String columnName) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(
                 "select count(*) from information_schema.columns "
                     + "where table_name = '" + tableName + "' and column_name = '" + columnName + "'")) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    private static String columnRemark(String tableName, String columnName) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(
                 "select remarks from information_schema.columns "
                     + "where table_name = '" + tableName + "' and column_name = '" + columnName + "'")) {
            rs.next();
            return StringUtils.trimToEmpty(rs.getString(1));
        }
    }

    @Test
    void createsTableWithInlineCommentInStandardMode() throws SQLException {
        execute("drop table if exists T_ORDER");
        SimpleTableInfo tableInfo = tableOf("T_ORDER",
            column("ORDER_NO", "VARCHAR", 64, true, null, "订单号"),
            column("REMARK", "VARCHAR", 255, false, null, "备注"));
        execute(ddlOpt.makeCreateTableSql(tableInfo, true));
        assertTrue(columnExists("T_ORDER", "ORDER_NO"));
        execute("drop table T_ORDER");
    }

    @Test
    void generatesH2NativeClausesInsteadOfMysqlModify() {
        TableField oldColumn = column("USER_NAME", "VARCHAR", 64, false, null, "用户名");
        TableField newColumn = column("USER_NAME", "VARCHAR", 128, true, "'admin'", "用户名称");
        List<String> sqlList = ddlOpt.makeModifyColumnSqls("T_USER", oldColumn, newColumn);
        assertEquals(4, sqlList.size());
        assertEquals("alter table T_USER alter column USER_NAME set data type VARCHAR(128)", sqlList.get(0));
        assertEquals("alter table T_USER alter column USER_NAME set not null", sqlList.get(1));
        assertEquals("alter table T_USER alter column USER_NAME set default 'admin'", sqlList.get(2));
        assertEquals("comment on column T_USER.USER_NAME is '用户名称'", sqlList.get(3));
    }

    @Test
    void dropsDefaultAndCommentWhenCleared() {
        TableField oldColumn = column("STATUS", "VARCHAR", 16, true, "'N'", "状态");
        TableField newColumn = column("STATUS", "VARCHAR", 16, true, null, "状态");
        List<String> sqlList = ddlOpt.makeModifyColumnSqls("T_USER", oldColumn, newColumn);
        assertEquals(List.of("alter table T_USER alter column STATUS drop default"), sqlList);
    }

    @Test
    void executesModifyClausesInStandardMode() throws SQLException {
        execute("drop table if exists T_USER");
        execute(ddlOpt.makeCreateTableSql(tableOf("T_USER",
            column("USER_NAME", "VARCHAR", 64, false, null, "用户名"),
            column("STATUS", "VARCHAR", 16, true, "'N'", "状态")), true));

        List<String> sqlList = ddlOpt.makeModifyColumnSqls("T_USER",
            column("USER_NAME", "VARCHAR", 64, false, null, "用户名"),
            column("USER_NAME", "VARCHAR", 128, true, "'admin'", "用户名称"));
        for (String sql : sqlList) {
            execute(sql);
        }
        assertEquals("用户名称", columnRemark("T_USER", "USER_NAME"));

        // 默认值清空：set default → drop default
        for (String sql : ddlOpt.makeModifyColumnSqls("T_USER",
            column("USER_NAME", "VARCHAR", 128, true, "'admin'", "用户名称"),
            column("USER_NAME", "VARCHAR", 128, true, null, "用户名称"))) {
            execute(sql);
        }
        execute("drop table T_USER");
    }

    @Test
    void executesRenameColumnInStandardMode() throws SQLException {
        execute("drop table if exists T_REN");
        execute(ddlOpt.makeCreateTableSql(tableOf("T_REN",
            column("OLD_NAME", "VARCHAR", 64, false, null, "旧名")), true));
        execute(ddlOpt.makeRenameColumnSql("T_REN", "OLD_NAME",
            column("NEW_NAME", "VARCHAR", 64, false, null, "新名")));
        assertFalse(columnExists("T_REN", "OLD_NAME"));
        assertTrue(columnExists("T_REN", "NEW_NAME"));
        execute("drop table T_REN");
    }

    @Test
    void ddlUtilsDelegatesToListClauses() {
        SimpleTableInfo oldTable = tableOf("T_MERGE",
            column("USER_NAME", "VARCHAR", 64, false, null, "用户名"));
        SimpleTableInfo newTable = tableOf("T_MERGE",
            column("USER_NAME", "VARCHAR", 64, true, null, "用户名"));
        List<String> sqlList = DDLUtils.makeAlterTableSqlList(newTable, oldTable, DBType.H2, null);
        assertEquals(List.of("alter table T_MERGE alter column USER_NAME set not null"), sqlList);
    }
}
