package com.centit.framework.jdbc.test.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.test.po.OfficeWorker;

import java.util.HashMap;
import java.util.Map;


public class OfficeWorkerDao extends BaseDaoImpl<OfficeWorker, String> {
    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        filterField.put("(like)workerName", "WORKER_NAME like :workerName");
        filterField.put("birthdayBegin(date)", "WORKER_BIRTHDAY>= :createDateBeg");
        filterField.put("(nextday)birthdayEnd", "WORKER_BIRTHDAY< :birthdayEnd");
        return filterField;
    }


}
