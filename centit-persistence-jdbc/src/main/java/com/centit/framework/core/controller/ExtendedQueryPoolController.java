package com.centit.framework.core.controller;

import com.centit.framework.core.dao.ExtendedQueryPool;
import com.centit.support.database.utils.DBType;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/sqlPool")
@Tag(name="持久化框架中的sql语句资源",
    description= "框架中的sql语句资源")
public class ExtendedQueryPoolController {

    protected Logger logger = LoggerFactory.getLogger(ExtendedQueryPoolController.class);
    @Value("${app.home:.}")
    private String appHome;

    @Value("${jdbc.url:}")
    private String jdbcUrl;

    @Value("${spring.datasource.url:}")
    private String springDatasourceUrl;

    public void setAppHome(String appHome) {
        this.appHome = appHome;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
    /**
     * 重新load Sql ExtendedMap
     * @return Map &lt; query_ID, 查询语句 &gt; 查询语句了列表
     */
    @Operation(summary = "重启加载[home]/sqlscript目录中的sql(*.xml)脚本文件", description = "重启加载sql语句")
    @RequestMapping(value = "/reloadextendedsqlmap", method = { RequestMethod.GET })
    @ResponseBody
    public Map<String,Object> reloadExtendedSqlMap() {
        boolean hasError = false;
        StringBuilder errorMsg = new StringBuilder();
        DBType dbType = DBType.mapDBType(
                StringUtils.isBlank(jdbcUrl)? springDatasourceUrl :jdbcUrl );
        try {
            ExtendedQueryPool.loadResourceExtendedSqlMap(dbType);
        } catch (DocumentException e) {
            hasError = true;
            errorMsg.append(e.getMessage());
            logger.error(e.getMessage(),e);
        }

        try {
            ExtendedQueryPool.loadExtendedSqlMaps(
                appHome +"/sqlscript",dbType);
        } catch (DocumentException | IOException e) {
            hasError = true;
            errorMsg.append(e.getMessage());
            logger.error(e.getMessage(),e);
        }

        Map<String,Object> res = new HashMap<>(6);
        if(hasError){
            res.put("code", 500);
            res.put("message", errorMsg.toString());
        }else {
            res.put("code", 0);
            res.put("message", "Reload Extended Sql Map succeed！");
        }
        return res;
    }
}
