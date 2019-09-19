package com.centit.framework.jdbc.test.server.impl;

import com.centit.framework.jdbc.test.dao.OfficeWorkerDao;
import com.centit.framework.jdbc.test.po.OfficeWorker;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.DatetimeOpt;

/**
 * 员工信息表
 */

public class OfficeWorkerServer  {
   public OfficeWorkerDao workerDao;

   public void test(){
       OfficeWorker worker = new OfficeWorker();

       workerDao.deleteObject(worker);
       workerDao.deleteObjectById(worker.getWorkerId());

       workerDao.deleteObjectForce(worker);
       workerDao.deleteObjectForceById(worker.getWorkerId());

       workerDao.deleteObjectsByProperties(CollectionsOpt.createHashMap(
               "sex","男" ,"workerBirthday",DatetimeOpt.createUtilDate(1980,12,12)
       ));

       workerDao.deleteObjectsForceByProperties(CollectionsOpt.createHashMap(
               "sex","男" ,"workerBirthday",DatetimeOpt.createUtilDate(1980,12,12)
       ));

       workerDao.updateObject(CollectionsOpt.createList("workerName"),worker);

       workerDao.mergeObject(worker);

       workerDao.getObjectById("");

       workerDao.listObjects();

       workerDao.listObjectsByProperties(CollectionsOpt.createHashMap(
               "sex","男" ,"workerBirthday",DatetimeOpt.createUtilDate(1980,12,12)));

       //workerDao.listObjectsAsJson(/*前端输入的查询参数*/map,/*PageDesc 分页信息 */pageDesc)
       //DatabaseOptUtils.listObjectsByParamsDriverSqlAsJson()
       //获取指定的懒加载字段
       workerDao.fetchObjectLazyColumn(worker,"headImage");
       //获取所有的懒加载字段
       workerDao.fetchObjectLazyColumns(worker);
   }
}
