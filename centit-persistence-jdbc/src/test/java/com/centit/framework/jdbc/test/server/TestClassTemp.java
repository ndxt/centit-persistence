package com.centit.framework.jdbc.test.server;

import com.alibaba.fastjson.JSON;
import com.centit.framework.jdbc.test.dao.OfficeWorkerDao;
import com.centit.framework.jdbc.test.po.OfficeWorker;
import com.centit.support.database.orm.JpaMetadata;
import com.centit.support.database.orm.OrmUtils;
import com.centit.support.database.orm.TableMapInfo;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by codefan on 17-8-31.
 */
public class TestClassTemp {
    public static void main(String[] args) throws NoSuchFieldException, SQLException, IOException {

        TableMapInfo mapInfo = JpaMetadata.fetchTableMapInfo(OfficeWorker.class);

        System.out.println(mapInfo.getReferences().get(0).getTargetEntityType());
        System.out.println(mapInfo.getReferences().get(0).getReferenceType());

        OfficeWorkerDao baseDao = new OfficeWorkerDao();
        System.out.println(baseDao.getPoClass().getTypeName());
        System.out.println(baseDao.getPkClass().getName());

        OfficeWorker worker = new OfficeWorker();

        worker = OrmUtils.prepareObjectForInsert(worker,mapInfo, null );

        System.out.println(JSON.toJSONString(worker));
    }
}
