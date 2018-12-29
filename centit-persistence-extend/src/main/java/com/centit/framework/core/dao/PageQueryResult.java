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
    // 是否做数据字段映射
    private boolean mapDictionary;
    // 属性过滤器，如果不为null则只有类表中的字段才会被返回
    private String[] filterFields;

    private PageQueryResult(){
    }

    private static  <D>  PageQueryResult<D>
    innerCreateResult(Collection<D> objList, PageDesc pageDesc, boolean mapDict, String[] filterFields){
        PageQueryResult result = new PageQueryResult();
        result.objList = objList;
        result.pageDesc = pageDesc;
        result.mapDictionary = mapDict;
        result.filterFields = filterFields;
        return result;
    }

    /**
     * 将对象转换为分页返回结果
     * @param objList 查询返回结果，需要 Collection 类型，并且类别中的对象不能为null
     * @param pageDesc 分页信息
     * @param filterFields 属性过滤器，只有这个列表中的属性才会被返回
     * @param <D> 对象类型
     * @return ToResponseData 用于 WarpUpResponseBody 的处理
     */
    public static  <D>  PageQueryResult<D>
        createResult(Collection<D> objList, PageDesc pageDesc, String[] filterFields){
        return innerCreateResult(objList, pageDesc, false, filterFields);
    }

    /**
     * 将对象转换为分页返回结果， 对象上如果有DictionaryMap注解的属性，将别数据转换为指定的属性添加到JSON中
     * @param objList 查询返回结果，需要 Collection 类型，并且类别中的对象不能为null
     * @param pageDesc 分页信息
     * @param filterFields 属性过滤器，只有这个列表中的属性才会被返回
     * @param <D> 对象类型
     * @return ToResponseData 用于 WarpUpResponseBody 的处理
     */
    public static  <D>  PageQueryResult<D>
        createResultMapDict(Collection<D> objList, PageDesc pageDesc, String[] filterFields){
        return innerCreateResult(objList, pageDesc, true, filterFields);
    }

    /**
     * 将对象转换为分页返回结果
     * @param objList 查询返回结果，需要 Collection 类型，并且类别中的对象不能为null
     * @param pageDesc 分页信息
     * @param <D> 对象类型
     * @return ToResponseData 用于 WarpUpResponseBody 的处理
     */
    public static  <D>  PageQueryResult<D>
        createResult(Collection<D> objList, PageDesc pageDesc){
        return innerCreateResult(objList, pageDesc, false, null);
    }

    /**
     * 将对象转换为分页返回结果， 对象上如果有DictionaryMap注解的属性，将别数据转换为指定的属性添加到JSON中
     * @param objList 查询返回结果，需要 Collection 类型，并且类别中的对象不能为null
     * @param pageDesc 分页信息
     * @param <D> 对象类型
     * @return ToResponseData 用于 WarpUpResponseBody 的处理
     */
    public static  <D>  PageQueryResult<D>
        createResultMapDict(Collection<D> objList, PageDesc pageDesc){
        return innerCreateResult(objList, pageDesc, true, null);
    }

    @Override
    public ResponseData toResponseData(){

        ResponseMapData respData = new ResponseMapData();
        if(this.mapDictionary){
            respData.addResponseData("objList",
                DictionaryMapUtils.objectsToJSONArray(this.objList, this.filterFields));
        } else {
            if(this.filterFields != null && this.filterFields.length > 0){
                respData.addResponseData("objList",
                    DictionaryMapUtils.objectsToJSONArrayNotMapDict(this.objList, this.filterFields));
            }else {
                respData.addResponseData("objList", this.objList);
            }
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
