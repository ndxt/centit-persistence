package com.centit.framework.core.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.dao.DataPowerFilter;
import com.centit.framework.core.service.DataScopePowerManager;

import java.util.List;

public class DataScopePowerManagerImpl implements DataScopePowerManager {
    /**
     * 获取用户数据权限过滤器
     * @param sUserCode sUserCode
     * @param sOptId 业务名称
     * @param sOptMethod 对应的方法名称
     * @return 过滤条件列表，null或者空位不过来
     */
    @Override
    public List<String> listUserDataFiltersByOptIdAndMethod(String topUnit, String sUserCode, String sOptId, String sOptMethod) {
        return CodeRepositoryUtil.listUserDataFiltersByOptIdAndMethod(topUnit, sUserCode, sOptId, sOptMethod);
    }

    @Override
    public DataPowerFilter createUserDataPowerFilter(JSONObject userInfo, String topUnit, String currentUnit) {
        DataPowerFilter dpf = new DataPowerFilter();
        //当前用户信息
        dpf.addSourceData("currentUser", userInfo);
        dpf.addSourceData("currentStation", currentUnit);

        CurrentUserContext context = new CurrentUserContext(userInfo, topUnit, currentUnit);
        dpf.addSourceData("primaryUnit", context::getPrimaryUnit);
        dpf.addSourceData("userUnits", context::listUserUnits);
        dpf.addSourceData("rankUnits", context::getRankUnitsMap);
        dpf.addSourceData("stationUnits", context::getStationUnitsMap);
        dpf.addSourceData("userRoles", context::listUserRoles);
        dpf.addSourceData("allSubUnits", context::listAllSubUnits);
        dpf.addSourceData("subUnits", context::listSubUnits);
        return dpf;
    }

}
