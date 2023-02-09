package com.centit.framework.jdbc.dao;

import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;

class DataFilter {
    public DataFilter(){

    }

    private String formule;
    private String pretreatment;

    private String valueName;
    private String filterSql;

    public DataFilter(String pretreatmentSql, String filterSql){
        this.filterSql = filterSql;
        setPretreatmentSql(pretreatmentSql);
    }

    /**
     * @param pretreatmentSql 表达式：(预处理,预处理2,......)参数名称
     * return new ImmutableTriple<>(paramName, paramAlias, paramPretreatment);
     */
    public void setPretreatmentSql(String pretreatmentSql) {
        ImmutableTriple<String, String, String> paramDesc = QueryUtils.parseParameter(pretreatmentSql);
        this.pretreatment = paramDesc.getRight();
        this.valueName = paramDesc.getMiddle();
        this.formule = paramDesc.getLeft();
        if(StringUtils.isBlank(formule)){
            formule = valueName;
        } else if(StringUtils.isBlank(valueName)){
            valueName = formule;
        }
    }

    public String getFormule() {
        return formule;
    }

    public String getPretreatment() {
        return pretreatment;
    }

    public String getValueName() {
        return valueName;
    }

    public String getFilterSql() {
        return filterSql;
    }

    public void setFilterSql(String filterSql) {
        this.filterSql = filterSql;
    }
}
