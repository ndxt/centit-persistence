package com.centit.support.database.ddl;

import com.alibaba.fastjson2.JSONArray;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqliteDDLOperations extends GeneralDDLOperations {


    public SqliteDDLOperations() {

    }

    public SqliteDDLOperations(Connection conn) {
        super(conn);
    }

    @Override
    public List<String> makeTableColumnComments(final TableInfo tableInfo, int commentContent){
        return new ArrayList<>();
    }

    @Override
    protected void appendPkSql(final TableInfo tableInfo, StringBuilder sbCreate) {
        if (tableInfo.hasParmaryKey()) {
            sbCreate.append(" primary key ");
            appendPkColumnSql(tableInfo, sbCreate);
        }
    }

    @Override
    public String makeModifyColumnSql(String tableCode, TableField oldColumn, TableField column) {
        return null;
    }

    private static void appendTableInfo(SimpleTableInfo tableInfo, Map<String, Object> object){
        if(object == null) return;
        for(Map.Entry<String, Object> ent : object.entrySet()) {
            SimpleTableField field = tableInfo.findFieldByName(ent.getKey());
            if(field==null){
                field = new SimpleTableField();
                field.setPropertyName(ent.getKey());
                field.setFieldLabelName(ent.getKey());
                field.setColumnName(FieldType.humpNameToColumn(ent.getKey(), true));
                if(ent.getValue()!=null) {
                    field.setFieldType(FieldType.mapToFieldType(ent.getValue().getClass()));
                    field.setColumnType(FieldType.mapToSqliteColumnType(field.getFieldType()));
                }
                tableInfo.addColumn(field);
            } else {
                if(StringUtils.isBlank(field.getColumnType()) && ent.getValue()!=null) {
                    field.setFieldType(FieldType.mapToFieldType(ent.getValue().getClass()));
                    field.setColumnType(FieldType.mapToSqliteColumnType(field.getFieldType()));
                }
            }
        }
    }

    public static SimpleTableInfo mapTableInfo(Map<String, Object> object, String tableName){
        SimpleTableInfo tableInfo = new SimpleTableInfo();
        tableInfo.setTableName(tableName);
        appendTableInfo(tableInfo, object);
        return tableInfo;
    }

    public static SimpleTableInfo mapTableInfo(List<Map<String, Object>> objList, String tableName){
        SimpleTableInfo tableInfo = new SimpleTableInfo();
        tableInfo.setTableName(tableName);
        for(Map<String, Object> objectMap : objList) {
            appendTableInfo(tableInfo, objectMap);
        }
        return tableInfo;
    }

    public static SimpleTableInfo mapTableInfo(JSONArray objArray, String tableName){
        SimpleTableInfo tableInfo = new SimpleTableInfo();
        tableInfo.setTableName(tableName);
        for(Object obj : objArray) {
            if(obj instanceof Map) {
                appendTableInfo(tableInfo, (Map<String, Object>)obj);
            }
        }
        return tableInfo;
    }

}
