package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
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
    public List<String> makeTableColumnComments(final TableInfo tableInfo, int commentContent){
        return new ArrayList<>();
    }

    @Override
    public String makeRenameColumnSql(final String tableCode, final String columnCode, final TableField column) {
/*        dropColumn(tableCode, columnCode);
        column.setColumnName(newColumnCode);
        addColumn(tableCode, column);*/
        return "exec sp_rename ' " + tableCode + "." + columnCode + "','" + column.getColumnName() + "','COLUMN'";
    }

    @Override
    public String makeModifyColumnSql(final String tableCode, final TableField oldColumn, final TableField column) {
        StringBuilder sbsql = new StringBuilder("alter table ");
        sbsql.append(tableCode);
        sbsql.append(" ALTER COLUMN ").append(column.getColumnName()).append(" ");
        if (!StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            || !GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            || !GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale())) {
            appendColumnTypeSQL(column, sbsql);
        }

        if (oldColumn.isMandatory() != column.isMandatory()) {
            sbsql.append(column.isMandatory() ? " not null" : " null");
        }
        return sbsql.toString();
    }
}
