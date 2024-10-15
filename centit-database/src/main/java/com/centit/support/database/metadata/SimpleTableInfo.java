package com.centit.support.database.metadata;

import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class SimpleTableInfo implements TableInfo {

    protected static final Logger logger = LoggerFactory.getLogger(SimpleTableInfo.class);
    /**
     * 包括主键
     */
    private List<SimpleTableField> columns = null;
    /**
     * oracle中的schema 其他数据库对应的是用户名
     */
    private String schema;
    /**
     * 表名
     */
    private String tableName;// 其实是table 代码 code
    /**
     * V ： 视图 T : table
     */
    private String tableType;

    /**
     * 表的中文名称
     */
    private String tableLabelName;// 表的 描述，中文名称
    /**
     * 表的备注信息
     */
    private String tableComment;// 表的备注信息
    /**
     * 表的主键名称
     */
    private String pkName;
    /**
     * 表的模块名称，对应java的包名称
     */
    private String packageName;
    /**
     * 默认排序字段
     */
    private String orderBy;

    /**
     * 引用关系，对应表的外建
     */
    private List<SimpleTableReference> references = null;

    public SimpleTableInfo() {
        this.tableType = "T";
    }

    public SimpleTableInfo(String tabname) {
        this.tableType = "T";
        setTableName(tabname);
    }

    protected static void writerXMLFile(Document doc, String xmlFile) {
        XMLWriter output;
        try {
            output = new XMLWriter(
                new FileWriter(new File(xmlFile)));
            output.write(doc);
            output.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
        }
    }

    /**
     * @return 数据库表名，对应pdm中的code，对应元数据中的 tabcode
     */
    public String getTableName() {
        return tableName;

    }

    public void setTableName(String tabName) {
        tableName = tabName;

    }

    /**
     * @return 数据库表中文名，对应pdm中的name,对应元数据中的 tabname
     */
    public String getTableLabelName() {
        return tableLabelName;
    }

    public void setTableLabelName(String tabDesc) {
        this.tableLabelName = tabDesc;
    }

    /**
     * @return 数据库表备注信息，对应pdm中的Comment,对应元数据中的 tabdesc
     */
    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tabComment) {
        this.tableComment = tabComment;
    }

    public String getPkName() {
        if (StringUtils.isBlank(pkName)) {
            return "PK_" + this.tableName;
        }
        return pkName;
    }

    public void setPkName(String pkName) {
        this.pkName = pkName;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    /**
     * 根据属性名查找 字段信息
     *
     * @param name 字段属性名
     * @return 字段信息
     */
    @Override
    public SimpleTableField findFieldByName(String name) {
        if(columns==null){
            return null;
        }
        for (SimpleTableField col : columns) {
            if (col.getPropertyName().equals(name))
                return col;
        }
        for (SimpleTableField col : columns) {
            if (col.getColumnName().equalsIgnoreCase(name))
                return col;
        }
        return null;
    }

    /**
     * 根据属性名查找 字段信息
     *
     * @param name 属性名
     * @return 字段信息
     */
    @Override
    public SimpleTableField findFieldByColumn(String name) {
        if(columns==null) return null;

        for (SimpleTableField col : columns) {
            if (col.getColumnName().equalsIgnoreCase(name))
                return col;
        }

        for (SimpleTableField col : columns) {
            if (StringUtils.equals(col.getPropertyName(), name))
                return col;
        }

        return null;
    }

    public List<SimpleTableField> getColumns() {
        if (columns == null)
            columns = new ArrayList<>(20);
        return columns;
    }

    public void setColumns(List<SimpleTableField> columns) {
        this.columns = columns;
    }

    public void addColumn(SimpleTableField column) {
        if (columns == null) {
            columns = new ArrayList<>(20);
        }
        columns.add(column);
    }

    public String getClassName() {
        String sClassName = FieldType.mapPropName(tableName);
        return sClassName.substring(0, 1).toUpperCase() +
            sClassName.substring(1);
    }

    public List<SimpleTableReference> getReferences() {
        if (references == null) {
            references = new ArrayList<>(4);
        }
        return references;
    }

    public void setReferences(List<SimpleTableReference> references) {
        this.references = references;
    }

    public boolean hasReferences() {
        return references != null && references.size() > 0;
    }

    public void addReference(SimpleTableReference reference) {
        if (references == null) {
            references = new ArrayList<>(4);
        }
        references.add(reference);
    }

    public SimpleTableReference findReference(String reference) {
        if (references == null)
            return null;

        for (SimpleTableReference ref : references) {
            if (ref.getReferenceName().equals(reference))
                return ref;
        }
        return null;
    }

    @Override
    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    /**
     * 将一个字段设为主键
     *
     * @param colname 字段
     */
    public void setColumnAsPrimaryKey(String colname) {
        SimpleTableField field = findFieldByName(colname);
        if (field != null) {
            field.setPrimaryKey(true);
        }
    }
}
