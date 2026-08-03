package com.centit.framework.jdbc.dao;

import com.centit.support.database.utils.ParamsDrivenSQL;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DataFilter {

    private String formula;
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
    private void setPretreatmentSql(String pretreatmentSql) {
        ImmutableTriple<String, String, String> paramDesc = ParamsDrivenSQL.parseParameter(pretreatmentSql);
        this.pretreatment = paramDesc.getRight();
        this.valueName = paramDesc.getMiddle();
        this.formula = paramDesc.getLeft();
        if(StringUtils.isBlank(formula)){
            formula = valueName;
        } else if(StringUtils.isBlank(valueName)){
            valueName = formula;
        }
    }

    public String getFormula() {
        return formula;
    }

    public String getPretreatment() {
        return pretreatment;
    }

    public void setPretreatment(String pretreatment) {
        this.pretreatment = pretreatment;
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
