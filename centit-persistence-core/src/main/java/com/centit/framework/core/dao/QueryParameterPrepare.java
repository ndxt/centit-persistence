package com.centit.framework.core.dao;

import com.centit.support.algorithm.NumberBaseOpt;

import java.util.Map;

/**
 * Created by codefan on 17-7-25.
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
    public final static Map<String, Object> prepPageParams
    (Map<String, Object> pageQureyMap, PageDesc pageDesc, int total) {

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
        pageQureyMap.put("startRow", new Integer(start));
        pageQureyMap.put("endRow", new Integer(end));
        pageQureyMap.put("maxSize", new Integer(pageSize));
        //System.err.println("pageQureyMap========"+JSON.toJSONString(pageQureyMap));
        return pageQureyMap;
    }

    public final static PageDesc fetckPageDescParams(Map<String, Object> pageQureyMap) {
        PageDesc pageDesc = new  PageDesc();
        Integer pageSize = NumberBaseOpt.castObjectToInteger(pageQureyMap.get("maxSize"));
        if(pageSize!=null)
            pageDesc.setPage( pageSize );

        Integer startRow = NumberBaseOpt.castObjectToInteger(pageQureyMap.get("startRow"));
        if( startRow != null && pageSize!=null && pageSize > 1 ){
            pageDesc.setPageNo( startRow/pageSize+1 );
        }else{
            pageDesc.setPageNo(1);
        }


        return pageDesc;
    }
}