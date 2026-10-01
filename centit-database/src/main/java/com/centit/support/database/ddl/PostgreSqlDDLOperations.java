package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.utils.FieldType;
import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class PostgreSqlDDLOperations extends GeneralDDLOperations {

    public PostgreSqlDDLOperations() {

    }

    public PostgreSqlDDLOperations(Connection conn) {
        super(conn);
    }

    @Override
    protected void appendColumnTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())) {
            StringBuilder dataType = new StringBuilder();
            appendColumnDataTypeSQL(field, dataType);
            if (StringUtils.equalsAnyIgnoreCase(dataType.toString(), "BIGINT", "INT8")) {
                sbCreate.append("BIGSERIAL");
            } else if (StringUtils.equalsAnyIgnoreCase(dataType.toString(), "SMALLINT", "INT2")) {
                sbCreate.append("SMALLSERIAL");
            } else {
                sbCreate.append("SERIAL");
            }
        } else {
            super.appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    protected void appendColumnDataTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())) {
            String type = StringUtils.trimToEmpty(field.getColumnType());
            if (StringUtils.equalsAnyIgnoreCase(type, "SMALLINT", "INT2", "INTEGER", "INT", "INT4", "BIGINT", "INT8")) {
                sbCreate.append(type);
            } else if (StringUtils.equalsIgnoreCase(type, "BIGSERIAL")) {
                sbCreate.append("BIGINT");
            } else if (StringUtils.equalsIgnoreCase(type, "SMALLSERIAL")) {
                sbCreate.append("SMALLINT");
            } else {
                sbCreate.append("INTEGER");
            }
        } else {
            super.appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    public String makeCreateSequenceSql(final String sequenceName) {
        return "create sequence " + QueryUtils.trimSqlIdentifier(sequenceName);
    }

    /**
     * 修改列定义 ，比如 修改 varchar 的长度
     *
     * @param tableCode 表代码
     * @param oldColumn 老的字段
     * @param column    字段
     * @return sql语句
     */
    @Override
    public List<String> makeModifyColumnSqls(String tableCode, TableField oldColumn, TableField column) {
        validateIdentityChange(oldColumn, column);
        List<String> sqlList = new ArrayList<>();
        if (!StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            || !GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            || !GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale())) {
            StringBuilder sbsql = new StringBuilder("alter table ");
            sbsql.append(tableCode)
                .append(" ALTER ").append(column.getColumnName()).append(" type ");
            appendColumnDataTypeSQL(column, sbsql);
            sqlList.add(sbsql.toString());
        }

        if (!FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType()) && oldColumn.isMandatory() != column.isMandatory()) {
            sqlList.add("alter table " + tableCode + " ALTER " + column.getColumnName()
                + (column.isMandatory() ? " set not null" : " drop not null"));
        }

        if (!StringUtils.equals(StringUtils.trimToEmpty(oldColumn.getDefaultValue()), StringUtils.trimToEmpty(column.getDefaultValue()))) {
            sqlList.add("alter table " + tableCode + " ALTER " + column.getColumnName()
                + (StringUtils.isBlank(column.getDefaultValue()) ? " drop default" : " set default " + column.getDefaultValue()));
        }
        if (!StringUtils.equals(StringUtils.defaultString(oldColumn.getFieldLabelName()), StringUtils.defaultString(column.getFieldLabelName()))) {
            sqlList.add("comment on column " + tableCode + "." + column.getColumnName() + " is '"
                + StringUtils.defaultString(column.getFieldLabelName()).replace("'", "''") + "'");
        }

        return sqlList;
    }

}
