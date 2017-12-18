package com.centit.framework.core.dao;

import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.TableMapInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Created by codefan on 17-7-25.
 * @author codefan
 */
@SuppressWarnings("unused")
public abstract class QueryParameterPrepare {
    /**
     * 处理翻页参数
     *
     * @param pageQureyMap pageQureyMap
     * @param pageDesc     pageDesc
     * @param total        total
     * @return Map类型
     */
    public static Map<String, Object> prepPageParams
    (Map<String, Object> pageQureyMap, com.centit.support.database.utils.PageDesc pageDesc, int total) {

        int pageNo = pageDesc.getPageNo()<1?1:pageDesc.getPageNo();
        int pageSize = pageDesc.getPageSize();
        if(total > 0) {
            int maxPageNo = (total - 1) / pageSize + 1;
            if (maxPageNo < pageNo) {
                pageNo = maxPageNo;// 页码校验
            }
            //回写总数量
            pageDesc.setTotalRows(total);
        }
        int start = (pageNo - 1) * pageSize;
        int end = pageNo * pageSize;
        pageQureyMap.put("startRow",start);
        pageQureyMap.put("endRow", end);
        pageQureyMap.put("maxSize",pageSize);
        //System.err.println("pageQureyMap========"+JSON.toJSONString(pageQureyMap));
        return pageQureyMap;
    }

    public static com.centit.support.database.utils.PageDesc fetckPageDescParams(Map<String, Object> pageQureyMap) {
        com.centit.support.database.utils.PageDesc pageDesc = new  com.centit.support.database.utils.PageDesc();
        Integer pageSize = NumberBaseOpt.castObjectToInteger(pageQureyMap.get("maxSize"));
        if(pageSize!=null) {
            pageDesc.setPageSize(pageSize);
        }

        Integer startRow = NumberBaseOpt.castObjectToInteger(pageQureyMap.get("startRow"));
        if( startRow != null && pageSize!=null && pageSize > 1 ){
            pageDesc.setPageNo( startRow/pageSize+1 );
        }else{
            pageDesc.setPageNo(1);
        }


        return pageDesc;
    }
    /**
     * 这个方法只是为了在框架中和 MyBatis 排序兼容，所对应的PO必须有jpa注解
     */
    public static Map<String, Object> makeMybatisOrderByParam
            (Map<String, Object> qureyParamMap, Class<?> ...clazzes){
        String sortField = StringBaseOpt.castObjectToString(
                qureyParamMap.get(CodeBook.TABLE_SORT_FIELD));
        if(StringUtils.isNotBlank(sortField)){
            for(Class clazz : clazzes) {
                TableMapInfo tableMapInfo = JpaMetadata.fetchTableMapInfo(clazz);
                SimpleTableField field = tableMapInfo.findFieldByName(sortField);
                if (field != null) {
                    sortField = field.getColumnName();
                    String orderDesc = StringBaseOpt.castObjectToString(qureyParamMap.get(CodeBook.TABLE_SORT_ORDER));
                    if ("desc".equalsIgnoreCase(orderDesc)) {
                        sortField = sortField + " desc";
                    }
                    qureyParamMap.put(CodeBook.MYBATIS_ORDER_FIELD, sortField);
                    return qureyParamMap;
                }
            }
        }

        return qureyParamMap;
    }

}