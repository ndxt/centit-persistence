package com.centit.framework.jdbc.test;

/**
 * Created by codefan on 17-8-31.
 */
public class TestClassTemp {
    public static void main(String[] args) {
        SimpleDao baseDao = new SimpleDao();
        System.out.println(baseDao.getPoClass().getTypeName());
        System.out.println(baseDao.getPkClass().getName());
    }
}
