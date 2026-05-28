package com.centit.support.database.metadata;

import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.xml.XMLObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dom4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PdmReader implements DatabaseMetadata {
    protected static final Logger logger = LoggerFactory.getLogger(PdmReader.class);

    private Document doc = null;
    private String sDBSchema = null;
    public boolean loadPdmFile(InputStream xmlStream) {
        try{
            doc = XMLObject.parseXmlStreamIgnoreDtd(xmlStream);
            return true;
        } catch (DocumentException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
            return false;
        }
    }

    public boolean loadPdmFile(String sPath) {
        try (FileInputStream is = new FileInputStream(new File(sPath))) {
            return loadPdmFile(is);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);//e.printStackTrace();
            return false;
        }
    }

    private QName getPdmQName(String sPreFix, String sName) {
        String uri = "attribute";
        // xmlns:a="attribute" xmlns:c="collection" xmlns:o="object">
        if ("c".equals(sPreFix))
            uri = "collection";
        else if ("o".equals(sPreFix))
            uri = "object";
        return new QName(sName, Namespace.get(sPreFix, uri), sPreFix + ':' + sName);
    }

    private String getElementText(Element e, String sPreFix, String sName) {
        if (e == null)
            return null;
        Element f = e.element(getPdmQName(sPreFix, sName));
        if (f == null)
            return null;
        //System.out.println(f.asXML());
        return f.getText();
    }

    private String getAttributeValue(Element e, String xPath) {
        if (e == null)
            return null;
        Attribute at = (Attribute) e.selectSingleNode(xPath);
        if (at == null)
            return null;
        return at.getValue();
    }

    /**
     * @return List Pair tableCode,tableName
     */
    public List<Pair<String, String>> getAllTableCode() {
        List<Pair<String, String>> tabNames = new ArrayList<>();
        List<Node> tabNodes = doc.selectNodes("//c:Tables/o:Table");
        for (Node tNode : tabNodes) {
            tabNames.add(new ImmutablePair<String, String>(
                getElementText((Element) tNode, "a", "Code"),
                getElementText((Element) tNode, "a", "Name")));
        }
        return tabNames;
    }

    public SimpleTableInfo getTableMetadata(String tabName) {
        List<String> pkColumnIDs = new ArrayList<>();
        if (doc == null)
            return null;
        SimpleTableInfo tab = new SimpleTableInfo(tabName.toUpperCase());
        if (sDBSchema != null)
            tab.setSchema(sDBSchema);

        Node nTab = doc.selectSingleNode("//c:Tables/o:Table[a:Code='" + tabName + "']");
        if (nTab == null)
            return null;
        //System.out.println(nTab.asXML());
        Element eTab = (Element) nTab;
        tab.setTableLabelName(getElementText(eTab, "a", "Name"));
        tab.setTableComment(getElementText(eTab, "a", "Comment"));
        //System.out.println(getElementText(eTab.element("a:Name")));
        Element elColumns = eTab.element(getPdmQName("c", "Columns"));///o:Column
        if (elColumns == null)
            return tab;
        //获取 表的字段列表
        List<Element> columns = elColumns.elements(getPdmQName("o", "Column"));///o:Column
        for (Element col : columns) {
            SimpleTableField field = new SimpleTableField();
            field.setColumnName(getElementText(col, "a", "Code"));

            //System.out.println(col.attributeValue("a:Code"));
            field.setColumnType(getElementText(col, "a", "DataType"));
            String stemp = getElementText(col, "a", "Length");
            if (stemp != null) {
                field.setMaxLength(NumberBaseOpt.castObjectToInteger(stemp, -1));
            }
            //PDM 中的这个定义和数据库中的好像不一致
            stemp = getElementText(col, "a", "Precision");
            if (stemp != null)
                field.setScale(Integer.valueOf(stemp));

            stemp = getElementText(col, "a", "Mandatory");
            if (stemp != null)
                field.setMandatory(stemp);
            field.setFieldLabelName(getElementText(col, "a", "Name"));
            field.setColumnComment(getElementText(col, "a", "Comment"));

            field.mapToMetadata();

            tab.addColumn(field);
        }

        //获取 表主键
        Attribute pkID = (Attribute) eTab.selectSingleNode("c:PrimaryKey/o:Key/@Ref");
        if (pkID == null)
            return tab;
        String sPkID = pkID.getValue();
        Element elPK = (Element) eTab.selectSingleNode("c:Keys/o:Key[@Id='" + sPkID + "']");
        if (elPK == null)
            return tab;
        tab.setPkName(getElementText(elPK, "a", "Code"));
        //tab.setPkName(pKCode);

        List<Node> pkColAttr = elPK.selectNodes("c:Key.Columns/o:Column/@Ref");
        for (Node pkCA : pkColAttr) {
            pkColumnIDs.add(((Attribute) pkCA).getValue());
            Element elPKCol = (Element) eTab.selectSingleNode(
                "c:Columns/o:Column[@Id='" + ((Attribute) pkCA).getValue() + "']/a:Code");
            if (elPKCol != null) {
                //System.out.println(elPKCol.asXML());
                tab.setColumnAsPrimaryKey(elPKCol.getText());
            }
        }
        //获取所有的外键
        List<Node> elReferences = (List<Node>) doc.selectNodes("//c:References/o:Reference[c:ParentKey/o:Key/@Ref='" + sPkID + "']");
        for (Node elRef : elReferences) {
            SimpleTableReference ref = new SimpleTableReference();
            ref.setParentTableName(tabName);
            ref.setReferenceCode(((Element) elRef).attributeValue("Id"));  //getElementText(elRef,"a","Code"));
            ref.setReferenceName(getElementText(((Element) elRef), "a", "Name"));

            String sChildTabID = getAttributeValue(((Element) elRef), "c:ChildTable/o:Table/@Ref"); //="o501" />
            if (sChildTabID == null)
                sChildTabID = getAttributeValue(((Element) elRef), "c:Object2/o:Table/@Ref"); //="o501" />

            Element eChildTab = null;
            if (sChildTabID != null) {
                eChildTab = (Element) doc.selectSingleNode("//c:Tables/o:Table[@Id='" + sChildTabID + "']");
            } else {
                String fpk = pkColumnIDs.get(0);
                String ffk = getAttributeValue(((Element) elRef),
                    "c:Joins/o:ReferenceJoin[c:Object1/o:Column/@Ref='" + fpk + "']/c:Object2/o:Column/@Ref");
                if (ffk != null)
                    eChildTab = (Element) doc.selectSingleNode("//c:Tables/o:Table[c:Columns/o:Column/@Id='" + ffk + "']");
            }
            if (eChildTab == null)
                continue;
            ref.setTableName(getElementText(eChildTab, "a", "Code"));

            for (String pkColID : pkColumnIDs) {
                String sChildColId = getAttributeValue(((Element) elRef),
                    "c:Joins/o:ReferenceJoin[c:Object1/o:Column/@Ref='" + pkColID + "']/c:Object2/o:Column/@Ref");
                //System.out.println(sChildColId);
                Element col = (Element) eChildTab.selectSingleNode("c:Columns/o:Column[@Id='" + sChildColId + "']");

                if (col == null)
                    continue;
                //System.out.println(col.asXML());
                String columnName = getElementText(col, "a", "Code");

                ref.addReferenceColumn(pkColID, columnName);
            }
            tab.addReference(ref);
        }
        return tab;
    }

    public String getDBSchema() {
        return sDBSchema;
    }

    public void setDBSchema(String schema) {
        if (schema != null)
            sDBSchema = schema.toUpperCase();
    }

    @Override
    public void setDBConfig(Connection dbc) {
        // not needed
    }

    @Override
    public List<SimpleTableInfo> listTables(boolean withColumn, String[] tableNames) {
        if (doc == null)
            return Collections.emptyList();
        List<SimpleTableInfo> tables = new ArrayList<>();
        List<Node> tabNodes = doc.selectNodes("//c:Tables/o:Table");
        for (Node tNode : tabNodes) {
            Element eTab = (Element) tNode;
            String tableCode = getElementText(eTab, "a", "Code");
            if (tableCode == null)
                continue;
            boolean canAddTable = false;
            if (tableNames == null) {
                canAddTable = true;
            } else {
                for (String tabName : tableNames) {
                    if (tabName.equalsIgnoreCase(tableCode)) {
                        canAddTable = true;
                        break;
                    }
                }
            }
            if (!canAddTable)
                continue;
            if (withColumn) {
                SimpleTableInfo tab = getTableMetadata(tableCode);
                if (tab != null) {
                    tab.setTableType("T");
                    tables.add(tab);
                }
            } else {
                SimpleTableInfo tab = new SimpleTableInfo(tableCode.toUpperCase());
                if (sDBSchema != null)
                    tab.setSchema(sDBSchema);
                tab.setTableLabelName(getElementText(eTab, "a", "Name"));
                tab.setTableComment(getElementText(eTab, "a", "Comment"));
                tab.setTableType("T");
                tables.add(tab);
            }
        }
        return tables;
    }

}
