package com.centit.framework.jdbc.test;

import com.centit.framework.jdbc.dao.BaseDaoImpl;

/**
 * Created by codefan on 17-8-31.
 */
public class SimpleDao extends BaseDaoImpl<SimplePo, String> {
    @Override
    public String getDaoEmbeddedFilter() {
        return null;
    }
}
