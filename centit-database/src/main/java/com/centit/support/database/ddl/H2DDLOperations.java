package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * H2 数据库 DDL 方言。建表、加列等语句与 MySQL 方言兼容（H2 支持列内联 comment）；
 * 修改列定义覆写为 H2 原生子句语法（SET DATA TYPE / SET NOT NULL 等），
 * 因此<b>不要求</b>连接开启 MySQL 兼容模式。
 * <p>
 * <a href="http://www.h2database.com/html/features.html#compatibility">...</a>
 */
public class H2DDLOperations extends MySqlDDLOperations {

    public H2DDLOperations() {

    }

    public H2DDLOperations(Connection conn) {
        super(conn);
    }

    @Override
    public String makeCreateSequenceSql(final String sequenceName) {
        return "create sequence " + QueryUtils.trimSqlIdentifier(sequenceName);
    }

    /**
     * H2 没有"整列定义替换"语法（MySQL 的 MODIFY COLUMN），按子句拆分为多条：
     * 类型/长度/精度、可空性、默认值、注释各自独立成句。
     */
    @Override
    public List<String> makeModifyColumnSqls(final String tableCode, final TableField oldColumn, final TableField column) {
        List<String> sqlList = new ArrayList<>();
        String columnPrefix = "alter table " + tableCode + " alter column " + column.getColumnName();
        if (!StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            || !GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            || !GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale())) {
            StringBuilder sbsql = new StringBuilder(columnPrefix).append(" set data type ");
            appendColumnTypeSQL(column, sbsql);
            sqlList.add(sbsql.toString());
        }
        if (oldColumn.isMandatory() != column.isMandatory()) {
            sqlList.add(columnPrefix + (column.isMandatory() ? " set not null" : " set null"));
        }
        if (!StringUtils.equals(StringUtils.trimToEmpty(oldColumn.getDefaultValue()),
            StringUtils.trimToEmpty(column.getDefaultValue()))) {
            sqlList.add(columnPrefix + (StringUtils.isBlank(column.getDefaultValue())
                ? " drop default" : " set default " + column.getDefaultValue().trim()));
        }
        if (!StringUtils.equals(StringUtils.trimToEmpty(oldColumn.getFieldLabelName()),
            StringUtils.trimToEmpty(column.getFieldLabelName()))
            && StringUtils.isNotBlank(column.getFieldLabelName())) {
            sqlList.add("comment on column " + tableCode + "." + column.getColumnName()
                + " is '" + column.getFieldLabelName().trim().replace("'", "''") + "'");
        }
        return sqlList;
    }

    @Override
    public String makeRenameColumnSql(final String tableCode, final String columnCode, final TableField column) {
        // H2 不支持 MySQL 的 CHANGE 语法
        return "alter table " + tableCode + " rename column " + columnCode
            + " to " + column.getColumnName();
    }

    /**
     * H2 标准模式的列定义中 default 必须位于 not null / comment 之前，
     * 与 MySQL 的拼接顺序不同，这里整体覆写。
     */
    @Override
    protected void appendColumnsSQL(final TableInfo tableInfo, StringBuilder sbCreate, boolean fieldStartNewLine) {
        if (tableInfo.getColumns() == null) {
            return;
        }
        boolean first = true;
        for (TableField field : tableInfo.getColumns()) {
            if (!first) {
                sbCreate.append(",");
            }
            first = false;
            if (fieldStartNewLine) {
                sbCreate.append("\r\n");
            }
            appendColumnSQL(field, sbCreate);
        }
    }

    @Override
    protected void appendColumnSQL(final TableField field, StringBuilder sbCreate) {
        sbCreate.append("  ").append(field.getColumnName()).append(" ");
        appendColumnTypeSQL(field, sbCreate);
        if (StringUtils.isNotBlank(field.getDefaultValue())) {
            sbCreate.append(" default ").append(field.getDefaultValue());
        }
        if (field.isMandatory()) {
            sbCreate.append(" not null");
        }
        if (StringUtils.isNotBlank(field.getFieldLabelName())) {
            sbCreate.append(" comment '").append(field.getFieldLabelName()).append("'");
        }
    }

}
