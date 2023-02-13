package com.centit.framework.jdbc.test.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.test.po.OfficeWorker;

import java.util.HashMap;
import java.util.Map;


public class OfficeWorkerDao extends BaseDaoImpl<OfficeWorker, String> {
    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        filterField.put("(like)g0_workerName", "WORKER_NAME like :workerName");
        filterField.put("birthdayBegin(date)", "WORKER_BIRTHDAY>= :createDateBeg");
        filterField.put("(nextday)birthdayEnd", "WORKER_BIRTHDAY< :birthdayEnd");
        filterField.put("tiaoChaoSanCiYiShang", "WORKER_ID in (select a.WORKER_ID from T_CAREER a group by a.WORKER_ID having count(*)>3)");
        return filterField;
    }


}
