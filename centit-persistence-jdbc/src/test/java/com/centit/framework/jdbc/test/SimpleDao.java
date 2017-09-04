package com.centit.framework.jdbc.test;

import com.centit.framework.jdbc.dao.BaseDaoImpl;

import java.util.Map;

/**
 * Created by codefan on 17-8-31.
 */
public class SimpleDao extends BaseDaoImpl<SimplePo, String> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }
}
