package com.centit.support.database.ddl;

import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringRegularOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.DatabaseAccess;
import com.centit.support.database.utils.FieldType;
import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class GeneralDDLOperations implements DDLOperations {

    protected Connection conn;

    public GeneralDDLOperations() {

    }

    public GeneralDDLOperations(Connection conn) {
        this.conn = conn;
    }

    public static GeneralDDLOperations createDDLOperations(final DBType dbtype) {
        switch (dbtype) {
            case Oracle:
            case DM:
            case KingBase:
            case GBase:
            case Oscar:
                return new OracleDDLOperations();
            case DB2:
                return new DB2DDLOperations();
            case SqlServer:
                return new SqlSvrDDLOperations();
            case MySql:
                return new MySqlDDLOperations();
            case H2:
                return new H2DDLOperations();
            case PostgreSql:
                return new PostgreSqlDDLOperations();
            case Sqlite:
                return new SqliteDDLOperations();
            case Access:
            default:
                throw new ObjectException("不支持的数据库类型：" + dbtype);
        }
    }

    public static GeneralDDLOperations createDDLOperations(final Connection conn)
        throws SQLException {
        DBType dbtype = DBType.mapDBType(conn.getMetaData().getURL());
        GeneralDDLOperations dllOperations = createDDLOperations(dbtype);
        dllOperations.setConnect(conn);
        return dllOperations;
    }

    /**
     * 返回格式检查结果
     *
     * @param tableInfo 表
     * @return Pair
     */
    public static Pair<Integer, String> checkTableWellDefined(final TableInfo tableInfo) {
        if (!Lexer.isLabel(tableInfo.getTableName())) {
            return new ImmutablePair<>(-1, "表名" + tableInfo.getTableName() + "格式不正确！");
        }
        if (tableInfo.getColumns() == null) {
            return new ImmutablePair<>(-5, "没有定义字段！");
        }
        for (TableField field : tableInfo.getColumns()) {
            if (!Lexer.isLabel(field.getColumnName())) {
                return new ImmutablePair<>(-2, "字段名" + field.getColumnName() + "格式不正确！");
            }
            if (StringUtils.isBlank(field.getColumnType())) {
                return new ImmutablePair<>(-3, "字段" + field.getColumnName() + "没有指定类型！");
            }
        }

        if (!tableInfo.hasParmaryKey()) {
            return new ImmutablePair<>(-4, "没有定义主键！");
        }

        return new ImmutablePair<>(0, "ok！");
    }

    public static Pair<Integer, String> checkViewWellDefined(final TableInfo tableInfo) {
        if (!Lexer.isLabel(tableInfo.getTableName())) {
            return new ImmutablePair<>(-1, "视图名" + tableInfo.getTableName() + "格式不正确！");
        }
        return new ImmutablePair<>(0, "ok！");
    }

    public static SimpleTableInfo parseDDL(final String createSql) {
        SimpleTableInfo tableInfo = new SimpleTableInfo();
        Lexer sql = new Lexer(createSql);
        String aWrod = sql.getAWord();
        //获取表名
        while(StringUtils.isNotBlank(aWrod) && !"table".equalsIgnoreCase(aWrod)){
            aWrod = sql.getAWord();
        }
        aWrod = sql.getAWord();
        aWrod = StringRegularOpt.trimString(aWrod);
        tableInfo.setTableName(aWrod);
        aWrod = sql.getAWord();
        while(StringUtils.isNotBlank(aWrod) && !"(".equalsIgnoreCase(aWrod)){
            aWrod = sql.getAWord();
        }
        aWrod = sql.getAWord();
        //获取字段
        while(StringUtils.isNotBlank(aWrod)){
            if(StringUtils.equalsAnyIgnoreCase(aWrod, "constraint", "primary", "key", "unique", "foreign", "check")){
                if ("constraint".equalsIgnoreCase(aWrod)) {
                    sql.getAWord(); // 约束名称
                    aWrod = sql.getAWord();
                }
                boolean primaryKey = "primary".equalsIgnoreCase(aWrod);
                if (!primaryKey) {
                    // 跳过非主键约束；括号内部的逗号不是列定义分隔符。
                    int depth = 0;
                    while (StringUtils.isNotBlank(aWrod)) {
                        if ("(".equals(aWrod)) {
                            depth++;
                        } else if (")".equals(aWrod)) {
                            if (depth == 0) {
                                break;
                            }
                            depth--;
                        } else if (",".equals(aWrod) && depth == 0) {
                            break;
                        }
                        aWrod = sql.getAWord();
                    }
                    if (",".equals(aWrod)) {
                        aWrod = sql.getAWord();
                        continue;
                    }
                    break;
                }
                int primaryKeyOrder = 1;
                while(StringUtils.isNotBlank(aWrod) && !"(".equalsIgnoreCase(aWrod)){
                    aWrod = sql.getAWord();
                }
                aWrod = sql.getAWord();
                aWrod = StringRegularOpt.trimString(aWrod);
                SimpleTableField column = tableInfo.findFieldByColumn(aWrod);
                if(primaryKey && column !=null){
                    column.setPrimaryKey(true);
                    column.setPrimaryKeyOrder(primaryKeyOrder);
                }
                aWrod = sql.getAWord();
                while(",".equals(aWrod)){
                    primaryKeyOrder++;
                    aWrod = sql.getAWord();
                    column = tableInfo.findFieldByColumn(aWrod);
                    if(primaryKey && column !=null){
                        column.setPrimaryKey(true);
                        column.setPrimaryKeyOrder(primaryKeyOrder);
                    }
                    aWrod = sql.getAWord();
                }
            } else { // VC_ID varchar(32) not null primary key comment 'VC_ID(主键)',
                SimpleTableField column = new SimpleTableField();
                aWrod = StringRegularOpt.trimString(aWrod);
                column.setColumnName(aWrod);
                aWrod = sql.getAWord();
                column.setColumnType(aWrod);
                aWrod = sql.getAWord();
                while(StringUtils.isNotBlank(aWrod) && !",".equals(aWrod)) {
                    if ("(".equals(aWrod)) {
                        aWrod = sql.getAWord();
                        column.setMaxLength(NumberBaseOpt.castObjectToInteger(aWrod, 0));
                        aWrod = sql.getAWord();
                        if (",".equals(aWrod)) {
                            aWrod = sql.getAWord();
                            column.setScale(NumberBaseOpt.castObjectToInteger(aWrod, 0));
                        } else if (!")".equals(aWrod)) {
                            sql.seekToRightBracket();
                        }
                    } else if ("not".equalsIgnoreCase(aWrod)) {
                        aWrod = sql.getAWord();
                        if ("null".equalsIgnoreCase(aWrod)) {
                            column.setMandatory(true);
                        }
                    } else if ("primary".equalsIgnoreCase(aWrod)) {
                        aWrod = sql.getAWord();
                        aWrod = StringRegularOpt.trimString(aWrod);
                        if ("key".equalsIgnoreCase(aWrod)) {
                            column.setPrimaryKey(true);
                            column.setPrimaryKeyOrder(1);
                        }
                    } else if (StringUtils.equalsAnyIgnoreCase(aWrod, "identity", "auto_increment")) {
                        column.setFieldType(FieldType.IDENTITY);
                    } else if ("comment".equalsIgnoreCase(aWrod)) {
                        aWrod = sql.getAWord();
                        aWrod = StringRegularOpt.trimString(aWrod);
                        column.setFieldLabelName(aWrod);
                        column.setColumnComment(aWrod);
                    }
                    aWrod = sql.getAWord();
                }
                tableInfo.addColumn(column);
            }

            while(StringUtils.isNotBlank(aWrod) && !",".equals(aWrod)){
                aWrod = sql.getAWord();
            }
            aWrod = sql.getAWord();
        }

        return tableInfo;
    }

    public void setConnect(Connection conn) {
        this.conn = conn;
    }

    /**
     * 创建序列
     * 仅支持方言原生序列，不使用表模拟。
     *
     * @param sequenceName 序列
     * @return sql语句
     */
    @Override
    public String makeCreateSequenceSql(final String sequenceName) {
        throw new UnsupportedOperationException("该数据库方言不支持原生 sequence");
    }

    protected void appendPkSql(final TableInfo tableInfo, StringBuilder sbCreate) {
        sbCreate.append("constraint ");
        if (StringUtils.isBlank(tableInfo.getPkName())) {
            sbCreate.append("pk_").append(tableInfo.getTableName());
        } else {
            sbCreate.append(tableInfo.getPkName());
        }
        sbCreate.append(" primary key ");
        appendPkColumnSql(tableInfo, sbCreate);
    }
    @Override
    public String makeCreateTableSql(final TableInfo tableInfo, boolean fieldStartNewLine) {
        StringBuilder sbCreate = new StringBuilder("create table ");
        sbCreate.append(tableInfo.getTableName()).append(" (");
        appendColumnsSQL(tableInfo, sbCreate, fieldStartNewLine);
        if (tableInfo.hasParmaryKey()) {
            sbCreate.append(",");
            if (fieldStartNewLine) {
                sbCreate.append("\r\n");
            }
            appendPkSql(tableInfo, sbCreate);
        }
        if (fieldStartNewLine) {
            sbCreate.append("\r\n");
        }
        sbCreate.append(")");
        return sbCreate.toString();
    }

    @Override
    public List<String> makeTableColumnComments(final TableInfo tableInfo, int commentContent){
        List<String> comments = new ArrayList<>();
        if(tableInfo.getColumns()==null)
            return comments;
        for (TableField field : tableInfo.getColumns()) {
            StringBuilder sbComment = new StringBuilder("COMMENT ON COLUMN ");
            sbComment.append(tableInfo.getTableName()).append(".")
                    .append(field.getColumnName()).append(" IS ");
            if(commentContent==1){
                sbComment.append(QueryUtils.buildStringForQuery(field.getFieldLabelName()));
            } else if(commentContent==2){
                sbComment.append(QueryUtils.buildStringForQuery(field.getColumnComment()));
            } else {
                sbComment.append(QueryUtils.buildStringForQuery(StringUtils.defaultString(field.getFieldLabelName())
                    + ":" + StringUtils.defaultString(field.getColumnComment())));
            }
            comments.add( sbComment.toString());
        }
        return comments;
    }

    protected void appendPkColumnSql(final TableInfo tableInfo, StringBuilder sbCreate) {
        sbCreate.append("(");
        int i = 0;
        for (TableField pkField : tableInfo.getPkFields()) {
            if (i > 0) {
                sbCreate.append(", ");
            }
            sbCreate.append(pkField.getColumnName());
            i++;
        }
        sbCreate.append(")");
    }

    protected void appendColumnsSQL(final TableInfo tableInfo, StringBuilder sbCreate, boolean fieldStartNewLine) {
        if(tableInfo.getColumns()==null)
            return;
        boolean first = true;
        for (TableField field : tableInfo.getColumns()) {
            if(!first){
                sbCreate.append(",");
            }
            first = false;
            if(fieldStartNewLine){
                sbCreate.append("\r\n");
            }
            appendColumnSQL(field, sbCreate);
        }
    }

    protected void appendColumnTypeSQL(final TableField field, StringBuilder sbCreate) {
        appendColumnDataTypeSQL(field, sbCreate);
    }

    /** 物理数据类型，不含 identity 等仅适用于建列的属性。 */
    protected void appendColumnDataTypeSQL(final TableField field, StringBuilder sbCreate) {
        sbCreate.append(field.getColumnType());
        String uppercaseColumnType = field.getColumnType().toUpperCase();
        //StringUtils.equalsIgnoreCase(str1, str2)
        if ("DATETIME YEAR TO FRACTION".equals(uppercaseColumnType)) {
            sbCreate.append("(").append(field.getMaxLength() > 0 ? field.getMaxLength() : 5).append(")");
        } else if (StringUtils.equalsAny(uppercaseColumnType, "VARBINARY", "BINARY", "BINARY VARYING")) {
            if (field.getMaxLength() > 0) {
                sbCreate.append("(").append(field.getMaxLength()).append(")");
            }
        } else if (uppercaseColumnType.contains("VARCHAR") || uppercaseColumnType.contains("VARYING")) {
            if (field.getMaxLength() > 0) {
                sbCreate.append("(").append(field.getMaxLength()).append(")");
            } else {
                sbCreate.append("(64)");
            }
        } else if (uppercaseColumnType.contains("CHAR")) {
            if (field.getMaxLength() > 0) {
                sbCreate.append("(").append(field.getMaxLength()).append(")");
            } else {
                sbCreate.append("(1)");
            }
        } else if (StringUtils.equalsAny(uppercaseColumnType,"NUMBER", "NUMERIC", "DECIMAL")) {
            if (field.getMaxLength() > 0) {
                sbCreate.append("(").append(field.getMaxLength());
            } else {
                sbCreate.append("(").append(24);
            }
            if (field.getScale() > 0) {
                sbCreate.append(",").append(field.getScale());
            }
            sbCreate.append(")");
        }
    }

    protected void appendColumnSQL(final TableField field, StringBuilder sbCreate) {
        sbCreate.append("  ").append(field.getColumnName())
            .append(" ");
        appendColumnTypeSQL(field, sbCreate);
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType()) && StringUtils.isNotBlank(field.getDefaultValue())) {
            throw new UnsupportedOperationException("identity 列不能同时指定默认值：" + field.getColumnName());
        }
        if (StringUtils.isNotBlank(field.getDefaultValue())) {
            sbCreate.append(" default ").append(field.getDefaultValue());
        }
        if (field.isMandatory()) {
            sbCreate.append(" not null");
        }
    }

    @Override
    public String makeDropTableSql(final String tableCode) {
        return "drop table " + tableCode;
    }

    @Override
    public String makeAddColumnSql(final String tableCode, final TableField column) {
        StringBuilder sbsql = new StringBuilder("alter table ");
        sbsql.append(tableCode);
        sbsql.append(" add column ");
        appendColumnSQL(column, sbsql);
        return sbsql.toString();
    }

    @Override
    public String makeDropColumnSql(final String tableCode, final String columnCode) {
        return "alter table " + tableCode + " drop COLUMN " + columnCode;

    }

    @Override
    public String makeRenameColumnSql(final String tableCode, final String columnCode, TableField column) {
        return "alter table " + tableCode +
            " rename COLUMN " +
            columnCode +
            " to " +
            column.getColumnName();
    }

    protected void validateIdentityChange(final TableField oldColumn, final TableField column) {
        boolean identity = FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType());
        if (identity != FieldType.IDENTITY.equalsIgnoreCase(oldColumn.getFieldType())) {
            throw new UnsupportedOperationException("不支持切换列的 identity 属性：" + column.getColumnName());
        }
        if (identity && StringUtils.isNotBlank(column.getDefaultValue())) {
            throw new UnsupportedOperationException("identity 列不能同时指定默认值：" + column.getColumnName());
        }
    }

    @Override
    public void createSequence(final String sequenceName) throws SQLException {
        DatabaseAccess.doExecuteSql(conn, makeCreateSequenceSql(sequenceName));
    }

    @Override
    public void createTable(TableInfo tableInfo) throws SQLException {
        DatabaseAccess.doExecuteSql(conn, makeCreateTableSql(tableInfo));
    }

    @Override
    public void dropTable(String tableCode) throws SQLException {
        DatabaseAccess.doExecuteSql(conn, makeDropTableSql(tableCode));
    }

    @Override
    public void addColumn(String tableCode, TableField column) throws SQLException {
        DatabaseAccess.doExecuteSql(conn, makeAddColumnSql(tableCode, column));
    }

    @Override
    public void modifyColumn(String tableCode, TableField oldColumn, TableField column) throws SQLException {
        for (String sql : makeModifyColumnSqls(tableCode, oldColumn, column)) {
            DatabaseAccess.doExecuteSql(conn, sql);
        }
    }

    @Override
    public void dropColumn(String tableCode, String columnCode) throws SQLException {
        DatabaseAccess.doExecuteSql(conn, makeDropColumnSql(tableCode, columnCode));
    }

    @Override
    public void renameColumn(String tableCode, String columnCode, TableField column) throws SQLException {
        DatabaseAccess.doExecuteSql(conn, makeRenameColumnSql(tableCode, columnCode, column));
    }

    @Override
    public String makeCreateViewSql(final String selectSql, final String viewName) {
        return "create or replace view " + viewName + " as " +
            selectSql;
    }

}
