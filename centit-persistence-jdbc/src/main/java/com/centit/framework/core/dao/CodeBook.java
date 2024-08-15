package com.centit.framework.core.dao;

import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;

/**
 * @author sx@centit.com
 * 这个代码是给hibernate中的条件过滤器用的，条件过滤器现在仍然可以用但是已经不推荐使用了。
 * codefan@sina.com 2017-8-29
 * @see com.centit.support.database.jsonmaptable.GeneralJsonObjectDao
 */
public class CodeBook {

    /*
     * like字段匹配的map名称
     */
    public static final String LIKE_HQL_ID = "LIKE";
    public static final String EQUAL_HQL_ID = "EQUAL";
    public static final String NO_PARAM_FIX = "NP_";
    public static final String IN_HQL_ID = "IN";
    /*
     * orderby
     */

    /**
     * 用户自定义排序描述，  放到 filterDesc 中
     */
    public static final String SELF_ORDER_BY = GeneralJsonObjectDao.SELF_ORDER_BY;

    public static final String SELF_ORDER_BY2 = GeneralJsonObjectDao.SELF_ORDER_BY2;
    /**
     * 用户自定义排序字段 ， 放到 filterDesc 中
     */
    public static final String TABLE_SORT_FIELD = GeneralJsonObjectDao.TABLE_SORT_FIELD;
    /**
     * 用户自定义排序字段的排序顺序 ， 放到 filterDesc 中
     */
    public static final String TABLE_SORT_ORDER = GeneralJsonObjectDao.TABLE_SORT_ORDER;
    /**
     * 这个参数只给 MyBatis 使用
     */
    public static final String MYBATIS_ORDER_FIELD = "mybatisOrderBy";

}
