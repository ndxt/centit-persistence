package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class MySqlDDLOperations extends GeneralDDLOperations {


    public MySqlDDLOperations() {

    }

    public MySqlDDLOperations(Connection conn) {
        super(conn);
    }

    @Override
    protected void appendColumnTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())) {
            appendColumnDataTypeSQL(field, sbCreate);
            sbCreate.append(" AUTO_INCREMENT");
        } else {
            super.appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    protected void appendColumnDataTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())) {
            String type = StringUtils.trimToEmpty(field.getColumnType());
            String baseType = StringUtils.removeEndIgnoreCase(type, " UNSIGNED");
            if (StringUtils.equalsAnyIgnoreCase(baseType, "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT")) {
                sbCreate.append(type);
            } else {
                sbCreate.append("bigint");
            }
        } else {
            super.appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    protected void appendPkSql(final TableInfo tableInfo, StringBuilder sbCreate) {
        sbCreate.append("primary key ");
        appendPkColumnSql(tableInfo, sbCreate);
    }

    @Override
    protected void appendColumnSQL(final TableField field, StringBuilder sbCreate) {
        sbCreate.append("  ");
        appendColumnDefinitionSQL(field, sbCreate);
    }

    @Override
    public List<String> makeTableColumnComments(final TableInfo tableInfo, int commentContent){
        return new ArrayList<>();
    }

    @Override
    public List<String> makeModifyColumnSqls(final String tableCode, final TableField oldColumn, final TableField column) {
        validateIdentityChange(oldColumn, column);
        if (StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            && GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            && GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale())
            && (FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType()) || column.isPrimaryKey() || oldColumn.isMandatory() == column.isMandatory())
            && StringUtils.equals(StringUtils.trimToEmpty(oldColumn.getDefaultValue()), StringUtils.trimToEmpty(column.getDefaultValue()))
            && StringUtils.equals(StringUtils.defaultString(oldColumn.getFieldLabelName()), StringUtils.defaultString(column.getFieldLabelName()))) {
            return List.of();
        }
        StringBuilder sbsql = new StringBuilder("alter table ").append(tableCode).append(" MODIFY COLUMN ");
        appendColumnDefinitionSQL(column, sbsql);
        return List.of(sbsql.toString());
    }

    private void appendColumnDefinitionSQL(final TableField column, StringBuilder sbsql) {
        if (FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType()) && StringUtils.isNotBlank(column.getDefaultValue())) {
            throw new UnsupportedOperationException("identity 列不能同时指定默认值：" + column.getColumnName());
        }
        sbsql.append(column.getColumnName()).append(" ");
        appendColumnTypeSQL(column, sbsql);
        if (StringUtils.isNotBlank(column.getDefaultValue())) {
            sbsql.append(" default ").append(column.getDefaultValue());
        }
        sbsql.append(column.isMandatory() || column.isPrimaryKey() || FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType()) ? " not null" : " null");
        sbsql.append(" comment '").append(StringUtils.defaultString(column.getFieldLabelName()).replace("'", "''")).append("'");
    }

    @Override
    public String makeRenameColumnSql(final String tableCode, final String columnCode, final TableField column) {
        StringBuilder sbsql = new StringBuilder("alter table ");
        sbsql.append(tableCode);
        sbsql.append(" CHANGE ");
        sbsql.append(columnCode);
        sbsql.append(" ");
        appendColumnDefinitionSQL(column, sbsql);
        return sbsql.toString();
    }

}
