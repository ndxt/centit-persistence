package com.centit.framework.core.dao;

import com.centit.support.database.utils.DBType;
import com.centit.support.file.FileSystemOpt;
import com.centit.support.xml.IgnoreDTDEntityResolver;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by codefan on 17-8-27.
 * @author codefan
 */
@SuppressWarnings("unused")
public abstract class ExtendedQueryPool {
    /**
     * 通过XML文件加载
     */
    private static final Map<String,String> EXTENDED_SQL_MAP=new HashMap<>(250);

    public static final void loadExtendedSqlMap(InputStream extendedSqlXmlFile, DBType dbtype)
            throws DocumentException {

        SAXReader builder = new SAXReader(false);
        builder.setValidation(false);
        builder.setEntityResolver(new IgnoreDTDEntityResolver());
        Document doc = builder.read(extendedSqlXmlFile);
        Element root = doc.getRootElement();//获取根元素
        for(Object element : root.elements()){
            String strDbType = ((Element)element).attributeValue("dbtype");
            if(StringUtils.isBlank(strDbType) || dbtype == DBType.valueOf(strDbType) ) {
                EXTENDED_SQL_MAP.put(
                        ((Element) element).attributeValue("id"),
                        ((Element) element).getStringValue());
            }
        }
    }

    public static final void loadResourceExtendedSqlMap(DBType dbtype)
            throws DocumentException {
        InputStream inputStream =
                ExtendedQueryPool.class.getResourceAsStream("/ExtendedSqlMap.xml");
        if(inputStream!=null) {
            loadExtendedSqlMap(inputStream, dbtype);
        }
    }

    public static final void loadExtendedSqlMaps(String filePath, DBType dbType)
        throws DocumentException,IOException {
        List<File> files = FileSystemOpt.findFilesByExt(filePath,"xml");
        if(files.size()>0){
            for(File file:files) {
                ExtendedQueryPool.loadExtendedSqlMap(
                    new FileInputStream(file),dbType
                );
            }
        }
    }

    public static final String getExtendedSql(String extendedSqlId){
        return EXTENDED_SQL_MAP.get(extendedSqlId);
    }

}
