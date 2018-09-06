package com.centit.framework.jdbc.test.server;

import com.centit.framework.jdbc.test.dao.OfficeWorkerDao;

/**
 * Created by codefan on 17-8-31.
 */
public class TestClassTemp {
    public static void main(String[] args) {
        OfficeWorkerDao baseDao = new OfficeWorkerDao();
        System.out.println(baseDao.getPoClass().getTypeName());
        System.out.println(baseDao.getPkClass().getName());
    }
}
