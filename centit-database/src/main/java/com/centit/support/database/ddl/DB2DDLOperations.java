package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.utils.FieldType;
import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class DB2DDLOperations extends GeneralDDLOperations {

    public DB2DDLOperations() {

    }

    public DB2DDLOperations(Connection conn) {
        super(conn);
    }

    @Override
    protected void appendColumnTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())) {
            appendColumnDataTypeSQL(field, sbCreate);
            sbCreate.append(" GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)");
        } else {
            super.appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    protected void appendColumnDataTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())
            && !StringUtils.equalsAnyIgnoreCase(field.getColumnType(), "SMALLINT", "INTEGER", "INT", "BIGINT", "DECIMAL", "NUMERIC")) {
            sbCreate.append("INTEGER");
        } else {
            super.appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    public String makeCreateSequenceSql(final String sequenceName) {
        return "CREATE SEQUENCE " + QueryUtils.trimSqlIdentifier(sequenceName) +
            " AS INTEGER START WITH 1 INCREMENT BY 1";
    }

    @Override
    public List<String> makeModifyColumnSqls(final String tableCode, final TableField oldColumn, final TableField column) {
        validateIdentityChange(oldColumn, column);
        List<String> sqlList = new ArrayList<>();
        if (!StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            || !GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            || !GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale())) {
            StringBuilder sbsql = new StringBuilder("alter table ");
            sbsql.append(tableCode)
                .append(" alter column ")
                .append(column.getColumnName())
                .append(" set data type ");
            appendColumnDataTypeSQL(column, sbsql);
            sqlList.add(sbsql.toString());
        }

        if (!FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType()) && oldColumn.isMandatory() != column.isMandatory()) {
            sqlList.add("alter table " + tableCode + " alter column " + column.getColumnName()
                + (column.isMandatory() ? " set not null" : " drop not null"));
        }

        if (!StringUtils.equals(StringUtils.trimToEmpty(oldColumn.getDefaultValue()), StringUtils.trimToEmpty(column.getDefaultValue()))) {
            sqlList.add("alter table " + tableCode + " alter column " + column.getColumnName()
                + (StringUtils.isBlank(column.getDefaultValue()) ? " drop default" : " set default " + column.getDefaultValue()));
        }
        if (!StringUtils.equals(StringUtils.defaultString(oldColumn.getFieldLabelName()), StringUtils.defaultString(column.getFieldLabelName()))) {
            sqlList.add("comment on column " + tableCode + "." + column.getColumnName() + " is '"
                + StringUtils.defaultString(column.getFieldLabelName()).replace("'", "''") + "'");
        }

        return sqlList;
    }

}
