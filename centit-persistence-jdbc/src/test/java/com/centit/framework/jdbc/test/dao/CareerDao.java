package com.centit.framework.jdbc.test.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.test.po.Career;

import java.util.Map;


public class CareerDao extends BaseDaoImpl<Career, String> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }

}
