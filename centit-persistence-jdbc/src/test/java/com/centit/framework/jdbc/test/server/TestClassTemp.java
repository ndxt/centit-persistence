package com.centit.framework.jdbc.test.server;

import com.alibaba.fastjson2.JSON;
import com.centit.framework.jdbc.test.dao.OfficeWorkerDao;
import com.centit.framework.jdbc.test.po.OfficeWorker;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.common.LeftRightPair;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmUtils;
import com.centit.support.database.orm.TableMapInfo;
import com.centit.support.database.utils.QueryAndNamedParams;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by codefan on 17-8-31.
 */
public class TestClassTemp {
    public static void main(String[] args) throws SQLException, IOException{

        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(OfficeWorker.class);

        System.out.println(mapInfo.getReferences().get(0).getTargetEntityType());
        System.out.println(mapInfo.getReferences().get(0).getReferenceFieldType());

        OfficeWorkerDao baseDao = new OfficeWorkerDao();
        System.out.println(baseDao.getPoClass().getTypeName());
        System.out.println(baseDao.getPkClass().getName());

        OfficeWorker worker = new OfficeWorker();

        worker = OrmUtils.prepareObjectForInsert(worker,mapInfo, null );

        System.out.println(JSON.toJSONString(worker));

        Map<String, Object> filterMap = CollectionsOpt.createHashMap("g0_workerName", "myName",
            "g0_workerName_ft", "search",
            "g0_createDate", new Date(), "birthdayBegin",  new Date(),
            "workerBirthday_lt", new Date(),
            "ball", "hello", "ct", "hello");
        List<String> fields = CollectionsOpt.createList("createDate","birthdayBegin","workerName", "WORKER_NAME","WORKER_NAME2");
        List<String> extentFilters = CollectionsOpt.createList(
            "([T_OFFICE_WORKER.WORKER_NAME] = {ball} and [T_OFFICE_WORKER.WORKER_SEX] = {ball})",
            "[T_OFFICE_WORKER.CREATE_DATE] > {ct}"
        );

        LeftRightPair<QueryAndNamedParams, TableField[]> query =
            baseDao.buildQueryByParamsWithFields(filterMap, fields, extentFilters, null);

        System.out.println(JSON.toJSONString(query));

        System.out.println(query.getLeft().getQuery());
    }
}
