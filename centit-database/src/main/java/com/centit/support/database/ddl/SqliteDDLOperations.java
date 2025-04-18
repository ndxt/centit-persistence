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
    public String makeCreateTableSql(final TableInfo tableInfo, boolean fieldStartNewLine) {
        StringBuilder sbCreate = new StringBuilder("create table ");
        sbCreate.append(tableInfo.getTableName()).append(" (");
        List<? extends TableField> pkColumns = tableInfo.getPkFields();
        int pkSum = pkColumns.size();
        TableField pkField;
        if(pkSum>0){
            pkField = pkColumns.get(0);
        } else {
            pkField = new SimpleTableField();
        }

        boolean first = true;
        for (TableField field : tableInfo.getColumns()) {
            if(!first){
                sbCreate.append(",");
            }
            first = false;
            if(fieldStartNewLine){
                sbCreate.append("\r\n");
            }
            appendColumnSQL(field, sbCreate);
            if (StringUtils.isNotBlank(field.getDefaultValue())) {
                sbCreate.append(" default ").append(field.getDefaultValue());
            }
            if(pkSum == 1 && StringUtils.equals(field.getColumnName(), pkField.getColumnName())){
                sbCreate.append(" primary key ");
                if(// StringUtils.equalsIgnoreCase("id", pkField.getColumnName()) &&
                    StringUtils.equalsIgnoreCase(pkField.getColumnType(), FieldType.INTEGER)) {
                    sbCreate.append("AUTOINCREMENT ");
                }
            }
        }
        if(pkSum>1) {
            sbCreate.append(", ");
            if(fieldStartNewLine){
                sbCreate.append("\r\n");
            }
            sbCreate.append("primary key ");
            appendPkColumnSql(tableInfo, sbCreate);
        }
        if(fieldStartNewLine){
            sbCreate.append("\r\n");
        }
        sbCreate.append(")");
        return sbCreate.toString();
    }

    @Override
    public String makeModifyColumnSql(String tableCode, TableField oldColumn, TableField column) {
        return null;
    }

    private static String mapPropertyNameToColumnName(String propertyName) {
        if(propertyName.indexOf('_')>=0){
            return propertyName.toUpperCase();// : propertyName;
        }
        return FieldType.humpNameToColumn(propertyName, true);
    }

    private static void appendTableInfo(SimpleTableInfo tableInfo, Map<String, Object> object){
        if(object == null) return;
        for(Map.Entry<String, Object> ent : object.entrySet()) {
            SimpleTableField field = tableInfo.findFieldByName(ent.getKey());
            if(field==null){
                field = new SimpleTableField();
                field.setPropertyName(ent.getKey());
                field.setFieldLabelName(ent.getKey());
                field.setColumnName(mapPropertyNameToColumnName(ent.getKey()));
                if(ent.getValue()!=null) {
                    field.setFieldType(FieldType.mapToFieldType(ent.getValue().getClass()));
                    field.setColumnType(FieldType.mapToSqliteColumnType(field.getFieldType()));
                } /*else {
                    field.setFieldType(FieldType.STRING);
                    field.setColumnType(FieldType.mapToSqliteColumnType(FieldType.STRING));
                }*/
                tableInfo.addColumn(field);
            } else {
                if(StringUtils.isBlank(field.getColumnType()) && ent.getValue()!=null) {
                    field.setFieldType(FieldType.mapToFieldType(ent.getValue().getClass()));
                    field.setColumnType(FieldType.mapToSqliteColumnType(field.getFieldType()));
                }
            }
        }
    }

    public static void setTablePrimaryKey(SimpleTableInfo tableInfo, String pkName, boolean allowFloat){
        SimpleTableField field = tableInfo.findFieldByName(pkName);
        if (field != null) {
            field.setPrimaryKey(true);
            // 不能用浮点数做主键
            if(!allowFloat && StringUtils.equalsAny(field.getFieldType(), FieldType.MONEY, FieldType.DOUBLE, FieldType.FLOAT ) ){
                field.setFieldType(FieldType.INTEGER);
                field.setColumnType(FieldType.mapToSqliteColumnType(FieldType.INTEGER));
            }
        }
    }

    private static void fixTableFields(SimpleTableInfo tableInfo){
        for(SimpleTableField field : tableInfo.getColumns()){
            if(StringUtils.isBlank(field.getColumnType())) {
                field.setFieldType(FieldType.STRING);
                field.setColumnType(FieldType.mapToSqliteColumnType(FieldType.STRING));
            }
        }
    }

    public static SimpleTableInfo mapTableInfo(Map<String, Object> object, String tableName){
        SimpleTableInfo tableInfo = new SimpleTableInfo();
        tableInfo.setTableName(tableName);
        appendTableInfo(tableInfo, object);
        fixTableFields(tableInfo);
        return tableInfo;
    }

    public static SimpleTableInfo mapTableInfo(List<Map<String, Object>> objList, String tableName){
        SimpleTableInfo tableInfo = new SimpleTableInfo();
        tableInfo.setTableName(tableName);
        for(Map<String, Object> objectMap : objList) {
            appendTableInfo(tableInfo, objectMap);
        }
        fixTableFields(tableInfo);
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
        fixTableFields(tableInfo);
        return tableInfo;
    }

}
