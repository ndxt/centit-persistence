package com.centit.support.test;

import com.centit.support.database.ddl.GeneralDDLOperations;
import com.centit.support.database.metadata.SimpleTableInfo;

public class TestDDLUtils {
    public static void main(String[] args) {
        String sql = "create table P_PRO_MEMBER_CONTRIBUTE_DEGREE (" +
            "  VC_ID varchar(32) not null comment 'VC_ID(主键)'," +
            "  VC_PRO_ID varchar(32) comment 'VC_PRO_ID(项目ID)'," +
            "  VC_TARGET_ID varchar(32) comment 'VC_TARGET_ID(标的ID)'," +
            "  VC_USER_ID varchar(32) comment 'VC_USER_ID(成员Id)',\n" +
            "  VC_USER_TYPE varchar(100) comment 'VC_USER_TYPE(成员类型代码)',\n" +
            "  L_BEFORE_INVEST_RATIO DOUBLE comment 'L_BEFORE_INVEST_RATIO(投前比例)',\n" +
            "  L_AFTER_INVEST_RATIO DOUBLE comment 'L_AFTER_INVEST_RATIO(投后比例)',\n" +
            "  L_QUIT_INVEST_RATIO DOUBLE comment 'L_QUIT_INVEST_RATIO(退出比例)',\n" +
            "  UPDATEDATE Date comment 'UPDATEDATE(更新时间)',\n" +
            "  UPDATOR varchar(32) comment 'UPDATOR(更新人)',\n" +
            "  L_STATE BIGINT comment 'L_STATE(状态)',\n" +
            "  primary key (VC_ID, VC_PRO_ID ));";
        SimpleTableInfo tableInfo = GeneralDDLOperations.parseDDL(sql);
        System.out.println(tableInfo.getTableName());
    }

}
