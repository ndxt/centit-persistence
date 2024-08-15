package com.centit.framework.jdbc.test.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.test.po.Career;
import com.centit.support.database.utils.QueryAndNamedParams;

import java.util.Collection;
import java.util.Map;

public class CareerDao extends BaseDaoImpl<Career, String> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }

    public QueryAndNamedParams buildQueryByParams(Map<String, Object> filterMap,
                                                     Collection<String> extentFilters) {
        return this.buildFilterByParams(filterMap, extentFilters, null);
    }
}
