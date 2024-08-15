package com.centit.framework.jdbc.test;

import com.centit.framework.jdbc.test.dao.CareerDao;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.database.utils.QueryAndNamedParams;


public class TestQuerySqlBuild {
    public static void main(String[] args) {
        // 测试查询语句生成
        CareerDao dao = new CareerDao();

        QueryAndNamedParams qap = dao.buildQueryByParams(CollectionsOpt.createHashMap("corporateName", "中科软"),
            CollectionsOpt.createList("[T_CAREER.beginDate] = {"));

        System.out.println(qap.getSql());
    }
}
