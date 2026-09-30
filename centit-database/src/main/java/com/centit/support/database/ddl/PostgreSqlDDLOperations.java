package com.centit.support.database.ddl;

import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.metadata.TableField;
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
        List<String> sqlList = new ArrayList<>();
        if (!StringUtils.equalsIgnoreCase(oldColumn.getColumnType(), column.getColumnType())
            || !GeneralAlgorithm.equals(oldColumn.getMaxLength(), column.getMaxLength())
            || !GeneralAlgorithm.equals(oldColumn.getScale(), column.getScale())) {
            StringBuilder sbsql = new StringBuilder("alter table ");
            sbsql.append(tableCode)
                .append(" ALTER ").append(column.getColumnName()).append(" type ");
            appendColumnTypeSQL(column, sbsql);
            sqlList.add(sbsql.toString());
        }

        if (oldColumn.isMandatory() != column.isMandatory()) {
            sqlList.add("alter table " + tableCode + " ALTER " + column.getColumnName()
                + (column.isMandatory() ? " set not null" : " drop not null"));
        }

        return sqlList;
    }


}
