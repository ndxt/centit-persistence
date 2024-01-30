package com.centit.support.test;

import com.centit.support.database.ddl.GeneralDDLOperations;
import com.centit.support.database.metadata.SimpleTableInfo;

public class TestDDLUtils {
    public static void main(String[] args) {
        String sql = "CREATE TABLE `history_version` (\n" +
            "  `history_id` varchar(32) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '主键id',\n" +
            "  `relation_id` varchar(32) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL COMMENT '关联表id',\n" +
            "  `os_id` varchar(32) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL COMMENT '应用id',\n" +
            "  `content` longtext CHARACTER SET utf8mb3 COLLATE utf8mb3_bin COMMENT '内容',\n" +
            "  `label` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL COMMENT '标签',\n" +
            "  `memo` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL COMMENT '备注',\n" +
            "  `push_time` datetime DEFAULT NULL COMMENT '提交时间',\n" +
            "  `push_user` varchar(32) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL COMMENT '提交人',\n" +
            "  `type` char(1) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL COMMENT '类型 1：工作流 2：页面设计 3：api网关',\n" +
            "  PRIMARY KEY (`history_id`) USING BTREE\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin;";
        SimpleTableInfo tableInfo = GeneralDDLOperations.parseDDL(sql);
        System.out.println(tableInfo.getTableName());
    }

}
