package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.utils.FieldType;
import com.centit.support.database.metadata.TableInfo;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

public class SqlSvrDDLOperations extends GeneralDDLOperations {


    public SqlSvrDDLOperations() {

    }

    public SqlSvrDDLOperations(Connection conn) {
        super(conn);
    }

    /*
     * EXEC sp_addextendedproperty
     * @name = N'MS_Description',  -- 注释的名称，MS_Description 是标准描述性注释名
     * @value = N'这里是您的注释',  -- 您想要添加的注释文本
     * @level0type = N'Schema', @level0name = 'dbo',  -- Schema名称，如果是默认Schema（通常是dbo），则可以省略这部分
     * @level1type = N'Table',  @level1name = 'YourTable', -- 表名
     * @level2type = N'Column', @level2name = 'YourColumn'; -- 字段名
     */
    @Override
    protected void appendColumnTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())) {
            appendColumnDataTypeSQL(field, sbCreate);
            sbCreate.append(" identity(1,1)");
        } else {
            appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    protected void appendColumnDataTypeSQL(final TableField field, StringBuilder sbCreate) {
        if (StringUtils.equalsAnyIgnoreCase(field.getColumnType(), "VARCHAR", "NVARCHAR", "VARBINARY") && field.getMaxLength() < 0) {
            sbCreate.append(field.getColumnType()).append("(MAX)");
        } else if (StringUtils.equalsAnyIgnoreCase(field.getColumnType(), "VARBINARY", "BINARY")) {
            sbCreate.append(field.getColumnType()).append("(").append(field.getMaxLength() > 0 ? field.getMaxLength() : 1).append(")");
        } else if (FieldType.IDENTITY.equalsIgnoreCase(field.getFieldType())
            && !StringUtils.equalsAnyIgnoreCase(field.getColumnType(), "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "NUMERIC", "DECIMAL")) {
            sbCreate.append("bigint");
        } else {
            super.appendColumnDataTypeSQL(field, sbCreate);
        }
    }

    @Override
    public List<String> makeTableColumnComments(final TableInfo tableInfo, int commentContent){
        throw new UnsupportedOperationException("SQL Server 列注释需要扩展属性，本方言暂不支持");
    }

    @Override
    public String makeRenameColumnSql(final String tableCode, final String columnCode, final TableField column) {
        return "exec sp_rename '" + (tableCode + "." + columnCode).replace("'", "''")
            + "','" + column.getColumnName().replace("'", "''") + "','COLUMN'";
    }

    @Override
    public String makeAddColumnSql(final String tableCode, final TableField column) {
        StringBuilder sql = new StringBuilder("alter table ").append(tableCode).append(" add ");
        appendColumnSQL(column, sql);
        return sql.toString();
    }

    @Override
    public String makeCreateViewSql(final String selectSql, final String viewName) {
        // 仅创建；视图已存在时由数据库报错，不模拟替换。
        return "create view " + viewName + " as " + selectSql;
    }

    @Override
    public List<String> makeModifyColumnSqls(final String tableCode, final TableField oldColumn, final TableField column) {
        validateIdentityChange(oldColumn, column);
        if (!StringUtils.equals(StringUtils.trimToEmpty(oldColumn.getDefaultValue()), StringUtils.trimToEmpty(column.getDefaultValue()))) {
            throw new UnsupportedOperationException("SQL Server 修改默认值需要默认约束名称，本方言暂不支持");
        }
        if (!StringUtils.equals(StringUtils.defaultString(oldColumn.getFieldLabelName()), StringUtils.defaultString(column.getFieldLabelName()))) {
            throw new UnsupportedOperationException("SQL Server 列注释需要扩展属性，本方言暂不支持");
        }
        boolean typeChanged = !StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            || !GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            || !GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale());
        boolean nullableChanged = !FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType())
            && oldColumn.isMandatory() != column.isMandatory();
        if (!typeChanged && !nullableChanged) {
            return List.of();
        }
        StringBuilder sbsql = new StringBuilder("alter table ");
        sbsql.append(tableCode);
        sbsql.append(" ALTER COLUMN ").append(column.getColumnName()).append(" ");
        appendColumnDataTypeSQL(column, sbsql);
        sbsql.append(column.isMandatory() || column.isPrimaryKey() || FieldType.IDENTITY.equalsIgnoreCase(column.getFieldType()) ? " not null" : " null");
        return List.of(sbsql.toString());
    }

}
