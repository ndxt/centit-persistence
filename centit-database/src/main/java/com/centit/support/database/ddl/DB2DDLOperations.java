package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
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
    public List<String> makeTableColumnComments(final TableInfo tableInfo, int commentContent){
        List<String> comments = new ArrayList<>();
        if(tableInfo.getColumns()==null)
            return comments;
        for (TableField field : tableInfo.getColumns()) {
            StringBuilder sbComment = new StringBuilder("ALTER TABLE ");
            sbComment.append(tableInfo.getTableName()).append(" ALTER COLUMN  ")
                .append(field.getColumnName()).append(" SET NOTE ");
            if(commentContent==1){
                sbComment.append('\'').append(field.getFieldLabelName()).append('\'');
            } else if(commentContent==2){
                sbComment.append('\'').append(field.getColumnComment()).append('\'');
            } else {
                sbComment.append('\'').append(field.getFieldLabelName()).append(':').append(field.getColumnComment()).append('\'');
            }
            comments.add( sbComment.toString());
        }
        return comments;
    }

    @Override
    public String makeCreateSequenceSql(final String sequenceName) {
        return "CREATE SEQUENCE " + QueryUtils.cleanSqlStatement(sequenceName) +
            " AS INTEGER START WITH 1 INCREMENT BY 1";
    }

    @Override
    public String makeModifyColumnSql(final String tableCode, final TableField oldColumn, final TableField column) {
        StringBuilder sbsql = new StringBuilder("alter table ");
        sbsql.append(tableCode);

        if (!StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            || !GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            || !GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale())) {
            sbsql.append(" alter column ")
                .append(column.getColumnName())
                .append(" set data type ");
            appendColumnTypeSQL(column, sbsql);
        }

        if (oldColumn.isMandatory() != column.isMandatory()) {
            sbsql.append(" alter column ")
                .append(column.getColumnName())
                .append(column.isMandatory() ? " set not null" : " drop not null");
        }

        return sbsql.toString();
    }
}
