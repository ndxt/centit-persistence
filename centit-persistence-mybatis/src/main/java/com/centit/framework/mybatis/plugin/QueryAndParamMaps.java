package com.centit.framework.mybatis.plugin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.centit.support.algorithm.ReflectionOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.compiler.Lexer;
import com.centit.support.database.utils.QueryAndNamedParams;
import com.centit.support.database.utils.QueryUtils;

import java.util.*;

public class QueryAndParamMaps {
    public String queryStmt;
    public List<String> paramNames;
    private Map<String, Object> namedParams;

    public String getQuery() {
        return queryStmt;
    }

    public void setQuery(String sql) {
        this.queryStmt = sql;
    }


    public List<String> getParamNames() {
        if(paramNames==null)
            paramNames = new ArrayList<>(4);
        return paramNames;
    }

    public void setParamName(List<String> paramNames){
        this.paramNames = paramNames;
    }

    public String getParamName(int index){
        if( paramNames==null)
            return null;
        if(index>=paramNames.size())
            return null;
        return paramNames.get(index);
    }

    public QueryAndParamMaps() {
        this.queryStmt = null;
        this.paramNames = null;
    }

    public QueryAndParamMaps(String sql) {
        this.queryStmt = sql;
        this.paramNames = null;
    }

    public QueryAndParamMaps(String sql,List<String> paramNames, Map<String, Object> params) {
        this.queryStmt = sql;
        this.paramNames = paramNames;
        this.namedParams = params;
    }

    public Map<String, Object> getNamedParams() {
        if(namedParams == null)
            namedParams = new HashMap<>();
        return namedParams;
    }

    public Object getParamValue(String paramName){
        if( namedParams ==null)
            return null;

        return namedParams.get(paramName);
    }

    public void setNamedParams(Map<String, Object> params) {
        this.namedParams = params;
    }

    public static QueryAndParamMaps createFromQueryAndNamedParams(String sql, Map<String,Object> namedParams){

        StringBuilder sqlb = new StringBuilder();
        List<String> paramNames = new ArrayList<>(namedParams.size()+5);
        Lexer lex = new Lexer(sql,Lexer.LANG_TYPE_SQL);

        int prePos = 0;
        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord)) {
            if (":".equals(aWord)) {

                int curPos = lex.getCurrPos();
                if(curPos-1>prePos)
                    sqlb.append(sql.substring(prePos, curPos-1));

                aWord = lex.getAWord();
                if (aWord == null || "".equals(aWord))
                    break;
                Object obj = namedParams.get(aWord);
                if(obj==null){
                    paramNames.add(aWord);
                    sqlb.append("?");
                }else if (obj instanceof Collection) {
                    int n=0;
                    for(Object po :(Collection<?>) obj){
                        if(n>0)
                            sqlb.append(",");
                        sqlb.append("?");
                        paramNames.add(aWord+"_"+n);
                        namedParams.put(aWord+"_"+n,po);
                        n++;
                    }
                } else if (obj instanceof Object[]) {
                    int n=0;
                    for(Object po :(Object[]) obj){
                        if(n>0)
                            sqlb.append(",");
                        sqlb.append("?");
                        paramNames.add(aWord+"_"+n);
                        namedParams.put(aWord+"_"+n,po);
                        n++;
                    }
                }else{
                    paramNames.add(aWord);
                    sqlb.append("?");
                }
                prePos = lex.getCurrPos();
            }

            aWord = lex.getAWord();
        }
        sqlb.append(sql.substring(prePos));

        return new QueryAndParamMaps(sqlb.toString(),
                paramNames,
                namedParams);
    }


    public static QueryAndParamMaps createFromQueryAndNamedParams(QueryAndNamedParams namedParamQuery){
        return createFromQueryAndNamedParams(
                namedParamQuery.getQuery(),namedParamQuery.getParams());
    }

    public static Map<String,Object> parameterObjectMap(Object object){
        if(object instanceof Map){
            return (Map<String,Object>) object;
        }
        if(ReflectionOpt.isScalarType(object.getClass())){
            return QueryUtils.createSqlParamsMap("param",object);
        }

        return (JSONObject) JSON.toJSON(object);
    }

    public static List<String> objectToStringList(Object object){
        if(object==null){
           return null;
        }else if (object instanceof Collection) {
            List<String> stringList = new ArrayList<>( ((Collection<?>) object).size()+1 );
            for(Object po :(Collection<?>) object){
                stringList.add(StringBaseOpt.castObjectToString(po));
            }
            return stringList;
        } else if (object instanceof Object[]) {
            List<String> stringList = new ArrayList<>( ((Object[]) object).length+1 );
            for(Object po :(Object[])  object){
                stringList.add(StringBaseOpt.castObjectToString(po));
            }
            return stringList;
        }//else{

        List<String> stringList = new ArrayList<>( 1 );
        stringList.add(StringBaseOpt.castObjectToString(object));
        return stringList;
        //}
    }
}
