package com.centit.framework.core.dao;

import com.centit.framework.components.SysUnitFilterEngine;
import com.centit.framework.components.SysUserFilterEngine;
import com.centit.framework.model.adapter.UserUnitVariableTranslate;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.ReflectionOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.algorithm.StringRegularOpt;
import com.centit.support.common.LeftRightPair;
import com.centit.support.compiler.Lexer;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.database.utils.QueryAndNamedParams;
import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.*;

@SuppressWarnings("unused")
public class DataPowerFilter implements UserUnitVariableTranslate {
    /**
     * 过滤条件中可以应用的数据，至少包括 userinfo 用户信息，unitinfo 用户主机构信息
     */
    private Map<String,Object> sourceData;

    public void setSourceDatas(Map<String,Object> sourceData) {
        this.sourceData = sourceData;
    }

    public Map<String,Object>  getSourceData() {
        return this.sourceData;
    }

    public void addSourceData(String name,Object data) {
        if(data==null)
            return;
        if(sourceData == null)
            sourceData = new HashMap<>();
        sourceData.put(name,data);
    }

    public void addSourceData(Object data) {
        if(data==null)
            return;
        if(sourceData == null)
            sourceData = new HashMap<>();
        sourceData.put(data.getClass().getSimpleName(),data);
    }

    public void addSourceDatas(Collection<Object> sourceData) {
        if(this.sourceData == null)
            this.sourceData = new HashMap<>();
        for(Object obj:sourceData){
            if(obj!=null)
                this.sourceData.put(obj.getClass().getSimpleName(), obj);
        }
    }

    public void addSourceDatas(Map<String,Object> paramMap) {
        if(this.sourceData == null)
            this.sourceData = new HashMap<>();
        this.sourceData.putAll(paramMap);
    }

    public void addSourceDatas(Object[] sourceData) {
        if(this.sourceData == null)
            this.sourceData = new HashMap<>();
        for(Object obj:sourceData){
            if(obj!=null)
                this.sourceData.put(obj.getClass().getSimpleName(), obj);
        }
    }

    public void setSourceDatas(Collection<Object> sourceData) {
        this.sourceData = new HashMap<>();
        for(Object obj:sourceData)
            this.sourceData.put(obj.getClass().getSimpleName(), obj);
    }

    public void setSourceDatas(Object[] sourceData) {
        this.sourceData = new HashMap<>();
        for(Object obj:sourceData)
            this.sourceData.put(obj.getClass().getSimpleName(), obj);
    }

    /**
     * 获取表达式敌营的值；这个地方需要根据业务的类型多样性和具体需求不断的完善
     * @param expression String
     * @return 表达式对应的值
     */
    public Object attainExpressionValue(String expression){
        return ReflectionOpt.attainExpressionValue(sourceData, expression);
    }

    @Override
    public Set<String> getUsersVariable(String s) {
        Set<String> retSet = new HashSet<>(20);
        Object obj = attainExpressionValue(s);
        if(obj instanceof Object[]) {
            Object[] objs = (Object[]) obj;
            if (objs.length > 0) {
                for (int i = 0; i < objs.length; i++) {
                    retSet.add(StringBaseOpt.objectToString(objs[i]));
                }
            }
        } else if(obj instanceof Collection){
            Collection<?> valueList = (Collection<?> )obj;
            for(Object ov : valueList){
                retSet.add(StringBaseOpt.objectToString(ov));
            }
        } else {
            retSet.add(StringBaseOpt.objectToString(obj));
        }
        return retSet;
    }

    @Override
    public Set<String> getUnitsVariable(String s) {
        return getUsersVariable(s);
    }

    @Override
    public Object getGeneralVariable(String s) {
        return attainExpressionValue(s);
    }

    protected static class DataPowerFilterTranslater implements QueryUtils.IFilterTranslater {
        private Map<String,String> tableAlias;
        private boolean jointSql;
        private DataPowerFilter dataPowerFilter;

        public DataPowerFilterTranslater(boolean jointSql,DataPowerFilter dataPowerFilter)
        {
            this.tableAlias = null;
            this.jointSql = jointSql;
            this.dataPowerFilter = dataPowerFilter;
        }

        @Override
        public void setTableAlias(Map<String, String> tableAlias) {
            this.tableAlias = tableAlias;
        }

        @Override
        public String translateColumn(String columnDesc){
            if(tableAlias==null||columnDesc==null||tableAlias.size()==0)
                return null;

            int n = columnDesc.indexOf('.');
            if(n<0){
                return tableAlias.get(columnDesc);
            }

            String poClassName = columnDesc.substring(0,n);
            String alias = tableAlias.get(poClassName);

            if(alias==null)
                return null;

            return "".equals(alias)? columnDesc.substring(n+1):alias+'.'+  columnDesc.substring(n+1);

        }

        /**
         * 变量转换器
         * @param paramName 变量表达式，可以式一个简单的变量，也可以式一个机构表达式，或者人员表达式
         * @return 返回对应的值
         */
        public Object mapParamFormula(String paramName){
            // 判断是否为机构表达式， 如果 首字母为 * 则为用户表达式
            if(paramName.contains("(")){
                String formula =  StringEscapeUtils.unescapeHtml4(paramName).trim();
                if(formula.startsWith("*")){
                    return SysUserFilterEngine.calcSystemOperators(
                        formula.substring(1), null, null, null, dataPowerFilter);
                }else {
                    return SysUnitFilterEngine.calcSystemUnitsByExp(
                        formula, null, dataPowerFilter);
                }
            }else {
                return dataPowerFilter.attainExpressionValue(paramName);
            }
        }

        @Override
        public LeftRightPair<String,Object> translateParam(String paramName){

            Object obj = mapParamFormula(paramName);

            if(obj==null)
                return null;
            if(obj instanceof String){
                if(StringUtils.isBlank((String) obj))
                    return null;
            }

            if(jointSql){
                return new LeftRightPair<>(
                        QueryUtils.buildObjectStringForQuery(obj), null);
            }else{
                return new LeftRightPair<>(
                        paramName, obj);
            }
        }

        @Override
        public String getVarValue(String varName) {
            Object res = mapParamFormula(varName);
            if(res==null)
                return "\"\"";
            return StringRegularOpt.quotedString(StringBaseOpt.objectToString(res));
        }

        @Override
        public String getLabelValue(String labelName) {
            return getVarValue(labelName);
        }

    }

    public DataPowerFilterTranslater getPowerFilterTranslater(){
        return new DataPowerFilterTranslater(false,this);
    }

    public DataPowerFilterTranslater getPowerFilterTranslater(boolean jointSql){
        return new DataPowerFilterTranslater(jointSql,this);
    }

    /**
     * @param queryStatement queryStatement
     * @param filters Collection filters
     * toSql 是否为sql语句，否：表示hql ，是：表示 sql
     * @param jointSql 变量内嵌在语句中，不用参数
     * @param isUnion 多个过滤之间是否是并集
     * @return translateQuery
     */
    public QueryAndNamedParams translateQuery(String queryStatement, Collection<String> filters,
                                              boolean jointSql, boolean isUnion){
        return QueryUtils.translateQuery(queryStatement,
                filters, isUnion, new DataPowerFilterTranslater(jointSql,this));
    }

    /**
     * 权限查询，不同的条件取并集
     * @param queryStatement queryStatement
     * @param filters Collection filters
     * @return translateSqlQuery
     */
    public QueryAndNamedParams translateSqlQuery
            (String queryStatement,Collection<String> filters){
        return translateQuery(queryStatement, filters,false, true);
    }

    /**
     * 视图过滤条件查询，不同的过滤条件取交接
     * @param queryStatement queryStatement
     * @param filters Collection filters
     * @return translateSqlFilterQuery
     */
    public QueryAndNamedParams translateSqlFilterQuery
            (String queryStatement,Collection<String> filters){
        return translateQuery(queryStatement, filters,false, false);
    }

    /**
     * 符合条件 返回1 否在 范围 -1
     * @param obj 验证对象
     * @param filter 过滤条件爱呢
     * @return 1 or -1
     */
    public int checkObjectFilter(Object obj,String filter){
        DataPowerFilterTranslater translater = getPowerFilterTranslater();
        String poClassName = obj.getClass().getSimpleName();
        Lexer varMorp = new Lexer();
        varMorp.setFormula(filter);
        StringBuilder checkStatement= new StringBuilder();
        String sWord = varMorp.getAWord();
        int prePos = 0;
        while( sWord!=null && ! "".equals(sWord) ){
            if( "[".equals(sWord)){
                int curPos = varMorp.getCurrPos();
                if(curPos-1>prePos)
                    checkStatement.append( filter.substring(prePos, curPos-1));
                varMorp.seekTo(']');
                prePos = varMorp.getCurrPos();
                String columnDesc =  filter.substring(curPos,prePos-1).trim();
                int n = columnDesc.indexOf('.');
                if(n<0)
                    return 0;

                String tempClassName = columnDesc.substring(0,n);
                if(!poClassName.equals(tempClassName))
                    return 0;

                Object fieldValue = ReflectionOpt.attainExpressionValue(obj, columnDesc.substring(n+1));

                checkStatement.append(QueryUtils.buildObjectStringForQuery(fieldValue));

            }else if( "{".equals(sWord)){
                int curPos = varMorp.getCurrPos();
                if(curPos-1>prePos)
                    checkStatement.append( filter.substring(prePos, curPos-1));
                varMorp.seekTo('}');
                prePos = varMorp.getCurrPos();
                String valueDesc =  filter.substring(curPos,prePos-1).trim();
                ImmutableTriple<String, String, String> paramMeta = QueryUtils.parseParameter(valueDesc);
                String paramName = StringUtils.isBlank(paramMeta.left) ? paramMeta.middle : paramMeta.left;
                Object fieldValue = translater.mapParamFormula(paramName);

                checkStatement.append(QueryUtils.buildObjectStringForQuery(fieldValue));
            }
            sWord = varMorp.getAWord();
        }
        checkStatement.append(filter.substring(prePos));

        return BooleanBaseOpt.castObjectToBoolean(
                VariableFormula.calculate(checkStatement.toString()),false)?
                1:-1;
    }

    public boolean checkObject(Object obj,Collection<String> filters){
        if(filters==null) return true;

        int nFalse=0;
        for(String filter : filters){
            int nRes =  checkObjectFilter(obj,filter);
            if(nRes==1) // 只要符合一个条件就可以
                return true;
            else if(nRes==-1) // 不符合条件 判断下一个
                nFalse++;
        }
        return nFalse == 0; // 如果 过滤条件为 空 也算合法
    }
}
