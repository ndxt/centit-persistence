package com.centit.framework.core.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.dao.DataPowerFilter;
import com.centit.framework.core.service.DataScopePowerManager;
import com.centit.framework.model.basedata.IUnitInfo;
import com.centit.framework.model.basedata.IUserRole;
import com.centit.framework.model.basedata.IUserUnit;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class DataScopePowerManagerImpl implements DataScopePowerManager {
    /**
     * 获取用户数据权限过滤器
     * @param sUserCode sUserCode
     * @param sOptId 业务名称
     * @param sOptMethod 对应的方法名称
     * @return 过滤条件列表，null或者空位不过来
     */
    @Override
    public List<String> listUserDataFiltersByOptIdAndMethod(String sUserCode, String sOptId, String sOptMethod) {
        return CodeRepositoryUtil.listUserDataFiltersByOptIdAndMethod(sUserCode,sOptId,sOptMethod);
    }

    @Override
    public DataPowerFilter createUserDataPowerFilter(JSONObject userInfo, String currentUnit) {
        DataPowerFilter dpf = new DataPowerFilter();
        //当前用户信息
        dpf.addSourceData("currentUser", userInfo);
        dpf.addSourceData("currentStation", currentUnit);

        CurrentUserContext context = new CurrentUserContext(userInfo, currentUnit);
        dpf.addSourceData("primaryUnit", (Supplier<IUnitInfo>)context::getPrimaryUnit);
        dpf.addSourceData("userUnits", (Supplier<List<? extends IUserUnit>>)context::listUserUnits);
        dpf.addSourceData("rankUnits", (Supplier<Map<String, List<IUserUnit>>>)context::getRankUnitsMap);
        dpf.addSourceData("stationUnits", (Supplier<Map<String, List<IUserUnit>>>)context::getStationUnitsMap);
        dpf.addSourceData("userRoles", (Supplier<List<? extends IUserRole>>)context::listUserRoles);
        dpf.addSourceData("allSubUnits", (Supplier<List<IUnitInfo>>)context::listAllSubUnits);
        dpf.addSourceData("subUnits", (Supplier<List<IUnitInfo>>)context::listSubUnits);
        return dpf;
    }

}
