package com.centit.support.database.utils;

import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.database.ddl.DDLOperations;
import com.centit.support.database.ddl.GeneralDDLOperations;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class DDLUtils {

    public static List<String> makeAlterTableSqlList(TableInfo newTable, TableInfo oldTable, DBType dbType, DDLOperations ddlOpt) {
        if(ddlOpt==null) {
            ddlOpt = GeneralDDLOperations.createDDLOperations(dbType);
        }
        List<String> sqlList = new ArrayList<>();
        if (oldTable == null) {
            sqlList.add(ddlOpt.makeCreateTableSql(newTable));
        } else {

            for (TableField  column : newTable.getColumns()) {
                TableField ocol = oldTable.findFieldByColumn(column.getColumnName());
                if (ocol == null) {
                    sqlList.add(ddlOpt.makeAddColumnSql(
                        newTable.getTableName(), column));
                } else {
                    if (StringUtils.equalsAnyIgnoreCase(column.getFieldType(), ocol.getFieldType())) {
                        boolean exits = !GeneralAlgorithm.equals(column.getMaxLength(), ocol.getMaxLength()) ||
                            !GeneralAlgorithm.equals(column.getScale(), ocol.getScale()) ||
                            !GeneralAlgorithm.equals(BooleanBaseOpt.castObjectToBoolean(column.isMandatory(),false), ocol.isMandatory()) ||
                            (!StringUtils.equals(column.getFieldLabelName(), ocol.getFieldLabelName())
                                && dbType.equals(DBType.MySql)); // 修改注释似乎没有意义
                        if (exits) {
                            sqlList.add(ddlOpt.makeModifyColumnSql(
                                newTable.getTableName(), ocol, column));
                        }
                    } else {
                        sqlList.addAll(ddlOpt.makeReconfigurationColumnSqls(
                            newTable.getTableName(), ocol.getColumnName(), column));
                    }
                }
            }
            for (TableField column : oldTable.getColumns()) {
                TableField pcol = newTable.findFieldByColumn(column.getColumnName());
                if (pcol == null) {
                    sqlList.add(ddlOpt.makeDropColumnSql(oldTable.getTableName(), column.getColumnName()));
                }
            }
        }
        return sqlList;
    }
}
