package com.centit.framework.core.dao;

import com.centit.framework.common.ResponseData;
import com.centit.framework.common.ResponseMapData;
import com.centit.framework.common.ToResponseData;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Collection;

@ApiModel(description = "分页查询返回结果")
public class PageQueryResult<T> implements ToResponseData {
    @ApiModelProperty(value = "查询结果")
    private Collection<T> objList;
    @ApiModelProperty(value = "分页信息")
    private PageDesc pageDesc;
    //是否做数据字段映射
    private boolean mapDictionary;

    private PageQueryResult(){
    }
    public static  <D>  PageQueryResult<D>
        createResult(Collection<D> objList, PageDesc pageDesc){
        PageQueryResult result = new PageQueryResult();
        result.objList = objList;
        result.pageDesc = pageDesc;
        result.mapDictionary = false;
        return result;
    }

    public static  <D>  PageQueryResult<D>
        createResultMapDict(Collection<D> objList, PageDesc pageDesc){
        PageQueryResult result = new PageQueryResult();
        result.objList = objList;
        result.pageDesc = pageDesc;
        result.mapDictionary = true;
        return result;
    }
    @Override
    public ResponseData toResponseData(){

        ResponseMapData respData = new ResponseMapData();
        if(this.mapDictionary){
            respData.addResponseData("objList",
                DictionaryMapUtils.objectsToJSONArray(this.objList));
        } else {
            respData.addResponseData("objList", this.objList);
        }
        respData.addResponseData("pageDesc", this.pageDesc);

        return respData;
    }

    public Collection<T> getObjList() {
        return objList;
    }

    public void setObjList(Collection<T> objList) {
        this.objList = objList;
    }

    public PageDesc getPageDesc() {
        return pageDesc;
    }

    public void setPageDesc(PageDesc pageDesc) {
        this.pageDesc = pageDesc;
    }
}
