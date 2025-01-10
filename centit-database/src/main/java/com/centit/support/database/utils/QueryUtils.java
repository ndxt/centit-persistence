package com.centit.support.database.utils;

import com.centit.support.algorithm.*;
import com.centit.support.common.LeftRightPair;
import com.centit.support.common.ObjectException;
import com.centit.support.compiler.EmbedFunc;
import com.centit.support.compiler.Lexer;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.compiler.VariableTranslate;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * @author codefan@hotmail.com
 */
@SuppressWarnings("unused")
public abstract class QueryUtils {

    /**
     * 表示这个参数不需要
     */
    public static final String SQL_PRETREAT_NO_PARAM = "NP";
    /**
     * 转化为模式匹配字符串，字符串中间的空格、tab都会被%替换
     */
    public static final String SQL_PRETREAT_LIKE = "LIKE";
    /**
     * 用于like语句，只在参数后面添加一个 %，MySql建议只用这个，其他的匹配方式在MySql中效率都比较低
     */
    public static final String SQL_PRETREAT_STARTWITH = "STARTWITH";
    /**
     * 用于like语句，只在参数前面添加一个 %
     */
    public static final String SQL_PRETREAT_ENDWITH = "ENDWITH";
    /**
     * 转化为日期类型, 没有时间， 处理效果和SQL_PRETREAT_TRUNC_DAY一致
     */
    public static final String SQL_PRETREAT_DATE = "DATE";
    /**
     * 转化为带时间的，日期的类型
     */
    public static final String SQL_PRETREAT_DATETIME = "DATETIME";
    /**
     * 转化为日期类型，并且计算第二天的日期，没有时间（时间为00:00:00） 用于区间查询的结束时间
     */
    public static final String SQL_PRETREAT_NEXT_DAY = "NEXTDAY";
    /**
     * 转化为日期类型，并且计算下一月的一日，没有时间（时间为00:00:00） 用于区间查询的结束时间
     */
    public static final String SQL_PRETREAT_NEXT_MONTH = "NEXTMONTH";
    /**
     * 转化为日期类型，并且计算下一年的一月一日，没有时间（时间为00:00:00） 用于区间查询的结束时间
     */
    public static final String SQL_PRETREAT_NEXT_YEAR = "NEXTYEAR";
    /**
     * 转化为日期类型，并且计算下一周周一，没有时间（时间为00:00:00） 用于区间查询的结束时间
     */
    public static final String SQL_PRETREAT_NEXT_WEEK = "NEXTWEEK";
    /**
     * 转化为日期类型，并且截断到当天，没有时间（时间为00:00:00）） 用于区间查询的开始时间
     */
    public static final String SQL_PRETREAT_TRUNC_DAY = "TRUNCDAY";
    /**
     * 转化为日期类型，并且截断到当月一日，没有时间（时间为00:00:00） 用于区间查询的开始时间
     */
    public static final String SQL_PRETREAT_TRUNC_MONTH = "TRUNCMONTH";
    /**
     * 转化为日期类型，并且截断到当年一月一日，没有时间（时间为00:00:00）用于区间查询的开始时间
     */
    public static final String SQL_PRETREAT_TRUNC_YEAR = "TRUNCYEAR";
    /**
     * 转化为日期类型，并且截断到本周周一，没有时间（时间为00:00:00）用于区间查询的开始时间
     */
    public static final String SQL_PRETREAT_TRUNC_WEEK = "TRUNCWEEK";

    /**
     * 转化为 2016-6-16这样的日期字符串
     */
    public static final String SQL_PRETREAT_DATESTR = "DATESTR";
    /**
     * 转化为 2016-6-16 10:25:34这样的日期和时间字符串
     */
    public static final String SQL_PRETREAT_DATETIMESTR = "DATETIMESTR";
    /**
     * 过滤掉所有非数字字符
     */
    public static final String SQL_PRETREAT_DIGIT = "DIGIT";
    /**
     * 大写
     */
    public static final String SQL_PRETREAT_UPPERCASE = "UPPERCASE";
    /**
     * 小写
     */
    public static final String SQL_PRETREAT_LOWERCASE = "LOWERCASE";

    /**
     * 转化为符合数字的字符串，
     */
    public static final String SQL_PRETREAT_NUMBER = "NUMBER";
    /**
     * 给子符串添加''使其可以拼接到sql语句中，并避免sql注入
     */
    public static final String SQL_PRETREAT_QUOTASTR = "QUOTASTR";
    /**
     * 应该转化 Integer类型，单对于数据库来说他和long没有区别所以也返回的Long类型
     */
    public static final String SQL_PRETREAT_INTEGER = "INTEGER";
    /**
     * 转化 Long 类型
     */
    public static final String SQL_PRETREAT_LONG = "LONG";
    /**
     * 转化为 Double 类型
     */
    public static final String SQL_PRETREAT_FLOAT = "FLOAT";
    /**
     * 将对象转换为 String， 如果是数组用 ','连接。
     */
    public static final String SQL_PRETREAT_STRING = "STRING";

    /**
     * 转化为驼峰结构， map_to_field
     */
    public static final String SQL_PRETREAT_MAPTOFIELD = "COLUMNTONAME";
    /**
     * 转化为下划线形式 ;
     * 将属性名转换为字段名
     */
    public static final String SQL_PRETREAT_MAP_NAME_COLUMN = "NAMETOCOLUMN";

    /**
     * 将字符串 用,分割返回 String[];对于支持数组变量的spring jdbcTemplate
     * 或者hibernate中的hql用这个处理就可以了，先腾实现的jpa也支持数组参数
     */
    public static final String SQL_PRETREAT_SPLITFORIN = "SPLITFORIN";
    /**
     * 对于不支持数组参数的执行引擎，需要将参数按照数值的格式进行扩展
     * 修改语句中的 命名参数，使其能够接受 多个参数以便用于in语句，比如： in(:a)
     * 传入a为一个数组，会根据a的实际长度变为 in(:a0,:a1,a2,......)
     */
    public static final String SQL_PRETREAT_CREEPFORIN = "CREEPFORIN";

    /**
     * 根据 数组变量 循环
     */
    public static final String SQL_PRETREAT_LOOP = "LOOP";

    /**
     * 根据 数组变量 循环 并且用 or 连接
     * 结果是  and （ sentence or  sentence or ....)
     * 这个预处理会自动添加  and () 所以语句开头不能添加 and
     */
    public static final String SQL_PRETREAT_LOOP_WITH_OR = "LOOPWITHOR";
    /**
     * 将参数值 拼接到 sql对应的参数位置，同时要避免sql注入；一般用与Order by中
     */
    public static final String SQL_PRETREAT_INPLACE = "INPLACE";
    /**
     * 过滤参数中的html标签
     */
    public static final String SQL_PRETREAT_ESCAPE_HTML = "ESCAPEHTML";

    private QueryUtils() {
        throw new IllegalAccessError("Utility class");
    }

    /**
     * 把字符串string包装成'string',并将字符传中的数里的"'"替换为“''”
     *
     * @param value value
     * @return 对应的'value'
     */
    public static String buildStringForQuery(String value) {
        if (value == null || "".equals(value))
            return "''";
        return "'" + StringUtils.replace(value.trim(), "'", "''") + "'";
    }

    public static String buildObjectsStringForQuery(Collection<?> objects) {
        if (objects == null || objects.isEmpty())
            return "''";
        StringBuilder sb = new StringBuilder();
        int dataCount = 0;
        for (Object obj : objects) {
            if (obj != null) {
                if (dataCount > 0)
                    sb.append(",");
                sb.append(buildObjectStringForQuery(obj));
                dataCount++;
            }
        }
        return dataCount == 0? "''" : sb.toString();
    }

    public static String buildObjectsStringForQuery(Object[] objects) {
        return buildObjectsStringForQuery(CollectionsOpt.arrayToList(objects));
    }

    public static String buildObjectStringForQuery(Object fieldValue) {
        if (fieldValue==null)
            return "''";
        if (fieldValue instanceof java.util.Date) {
            return QueryUtils.buildDatetimeStringForQuery((java.util.Date) fieldValue);
        } else if (fieldValue.getClass().getSuperclass().equals(Number.class)) {
            return fieldValue.toString();
        } else if (fieldValue instanceof Object[]) {
            return QueryUtils.buildObjectsStringForQuery((Object[]) fieldValue);
        } else if (fieldValue instanceof Collection<?>) {
            return QueryUtils.buildObjectsStringForQuery((Collection<?>) fieldValue);
        } else {
            return QueryUtils.buildStringForQuery(fieldValue.toString());
        }
    }

    public static String buildDateStringForQuery(Date value) {
        return "'" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd")
            + "'";
    }

    public static String buildDateStringForQuery(java.sql.Date value) {
        return "'" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd")
            + "'";
    }

    public static String buildDatetimeStringForQuery(Date value) {
        return "'" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd HH:mm:ss")
            + "'";
    }

    public static String buildDatetimeStringForQuery(java.sql.Date value) {
        return "'" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd HH:mm:ss")
            + "'";
    }

    /**
     * 在HQL检索策略以外,模糊拼接key-value键值对,把string包装成to-char('value','yyyy-MM-dd')
     *
     * @param value value
     * @return 对应的to-char('value','yyyy-MM-dd')
     */
    public static String buildDateStringForOracle(Date value) {
        return "TO_DATE('" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd")
            + "','yyyy-MM-dd')";
    }

    public static String buildDateStringForOracle(java.sql.Date value) {
        return "TO_DATE('" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd")
            + "','yyyy-MM-dd')";
    }


    /**
     * 在HQL检索策略以外,模糊拼接key-value键值对,把string包装成to-char('value','yyyy-MM-dd
     * hh24:mi:ss')
     *
     * @param value value
     * @return 对应的to-char('value','yyyy-MM-dd hh24:mi:ss')
     */
    public static String buildDateTimeStringForOracle(java.util.Date value) {
        return "TO_DATE('" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd HH:mm:ss")
            + "','yyyy-MM-dd hh24:mi:ss')";
    }

    public static String buildDateTimeStringForOracle(java.sql.Date value) {
        return "TO_DATE('" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd HH:mm:ss")
            + "','yyyy-MM-dd hh24:mi:ss')";
    }

    /**
     * 将string中的 空格换成 % 作为like语句的匹配串
     * 比如在客户端输入 “hello world”，会转变为  "%hello%world%"，即将头尾和中间的空白转换为%用于匹配。
     *
     * @param sMatch sMatch
     * @return String
     */
    public static String getMatchString(String sMatch) {
        StringBuilder sRes = new StringBuilder("%");
        char preChar = '%', curChar;
        int sL = sMatch.length();
        for (int i = 0; i < sL; i++) {
            curChar = sMatch.charAt(i);
            if ((curChar == ' ') || (curChar == '\t') || (curChar == '%') || (curChar == '*')) {
                curChar = '%';
                if (preChar != '%') {
                    sRes.append(curChar);
                    preChar = curChar;
                }
            } else if (curChar == '?') {
                //|| curChar == '\'' || curChar == '\"' || curChar == '<' || curChar == '>') {
                sRes.append("_");
                preChar = curChar;
            } else {
                sRes.append(curChar);
                preChar = curChar;
            }
        }
        if (preChar != '%')
            sRes.append('%');
        return sRes.toString();
    }

    /**
     * 将查序变量中 用于 like语句的变量转换为match字符串，比如“hello world”会转变为  "%hello%world%"，
     *
     * @param queryParams 查询命名变量和值对
     * @param likeParams  用于like 的变量名
     * @return 返回在查询变量中找到的like变量
     */
    public static int replaceMatchParams(Map<String, Object> queryParams, Collection<String> likeParams) {
        if (likeParams == null || likeParams.size() == 0 || queryParams == null)
            return 0;
        int n = 0;
         for (String f : likeParams) {
            Object value = queryParams.get(f);
            if (value != null) {
                queryParams.put(f, getMatchString(StringBaseOpt.objectToString(value)));
                n++;
            }
        }
        return n;
    }

    /**
     * 将查序变量中 用于 like语句的变量转换为match字符串，比如“hello world”会转变为  "%hello%world%"，
     *
     * @param queryParams 查询命名变量和值对
     * @param likeParams  用于like 的变量名
     * @return 返回在查询变量中找到的like变量
     */
    public static int replaceMatchParams(Map<String, Object> queryParams, String... likeParams) {
        if (likeParams == null || likeParams.length == 0 || queryParams == null)
            return 0;
        int n = 0;
        for (String f : likeParams) {
            Object value = queryParams.get(f);
            if (value != null) {
                queryParams.put(f, getMatchString(StringBaseOpt.objectToString(value)));
                n++;
            }
        }
        return n;
    }


    /**
     * 去掉 order by 语句
     *
     * @param sql sql
     * @return sql
     */
    public static boolean hasOrderBy(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord) && !"order".equalsIgnoreCase(aWord)) {
            aWord = lex.getAWord();
        }
        return "order".equalsIgnoreCase(aWord);
    }

    /**
     * 去掉 order by 语句
     *
     * @param sql sql
     * @return sql
     */
    public static String removeOrderBy(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String aWord = lex.getAWord();
        int nPos = lex.getCurrPos();
        while (aWord != null && !"".equals(aWord) && !"order".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
            }
            nPos = lex.getCurrPos();
            aWord = lex.getAWord();
            if (aWord == null || "".equals(aWord))
                return sql;
        }
        return sql.substring(0, nPos);
    }

    /**
     * 去掉 order by 语句
     *
     * @param sql sql
     * @return sql
     */
    public static String getGroupByField(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String aWord = lex.getAWord();

        while (aWord != null && !"".equals(aWord) && !"group".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
                //aWord = lex.getAWord();
            }
            aWord = lex.getAWord();
            if (aWord == null || "".equals(aWord))
                return null;

        }
        if ("group".equalsIgnoreCase(aWord)) {
            while (aWord != null && !"".equals(aWord) && !"by".equalsIgnoreCase(aWord)) {
                aWord = lex.getAWord();
            }
        }
        if (!"by".equalsIgnoreCase(aWord))
            return null;
        int nPos = lex.getCurrPos();
        int nEnd = nPos;

        while (aWord != null && !"".equals(aWord) && !"order".equalsIgnoreCase(aWord)) {
            nEnd = lex.getCurrPos();
            aWord = lex.getAWord();
        }
        if (nEnd > nPos)
            return sql.substring(nPos, nEnd);
        return null;
    }


    /**
     * 将sql语句  filed部分为界 分三段；
     * 第一段为 select 之前的内容，如果是sql server 将包括  top [n] 的内容
     * 第二段为 from 和select 之间的内容，就是field内容
     * 第三段为 where  内容包括 order by
     * @param sql sql
     * @return sql
     */
    public static List<String> splitSqlByFields(String sql) {

        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        List<String> sqlPiece = new ArrayList<>(5);
        int sl = sql.length();
        String aWord = lex.getAWord();

        while (aWord != null && !"".equals(aWord) && !"select".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
            }
            aWord = lex.getAWord();
            if (aWord == null || "".equals(aWord))
                break;
        }

        int nSelectPos = lex.getCurrPos();
        int nFieldBegin = nSelectPos;

        if (nSelectPos >= sl) {
            lex.setFormula(sql);
            nSelectPos = 0;
            nFieldBegin = 0;
            aWord = lex.getAWord();
        } else {
            //特别处理sql server 的 top 语句
            aWord = lex.getAWord();
            if ("top".equalsIgnoreCase(aWord)) {
                aWord = lex.getAWord();
                if (StringRegularOpt.isNumber(aWord))
                    nFieldBegin = lex.getCurrPos();
            }
        }

        while (aWord != null && !"".equals(aWord) && !"from".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
            }
            aWord = lex.getAWord();
            if (aWord == null || "".equals(aWord))
                return sqlPiece;
        }
        int nFieldEnd = lex.getCurrPos();

        sqlPiece.add(sql.substring(0, nSelectPos));
        sqlPiece.add(sql.substring(nFieldBegin, nFieldEnd));
        sqlPiece.add(sql.substring(nFieldEnd));
        if (nFieldBegin > nSelectPos) { // 只有 sqlserver 有 top 字句的语句 才有这部分
            sqlPiece.add(sql.substring(nSelectPos, nFieldBegin));
        }

        return sqlPiece;
    }

    /**
     * 将查询语句转换为相同条件的查询符合条件的记录数的语句, 需要考虑with语句
     * 即将 select 的字段部分替换为 count(*) 并去掉 order by排序部分
     * 对查询语句中有distinct的sql语句不使用
     *
     * @param sql sql
     * @return sql
     */
    public static String buildGetCountSQLByReplaceFields(String sql) {
        List<String> sqlPieces = splitSqlByFields(sql);
        if (sqlPieces == null || sqlPieces.size() < 3)
            return "";
        if (StringUtils.isBlank(sqlPieces.get(0))) {
            sqlPieces.set(0, "select");
        }

        String groupByField = QueryUtils.getGroupByField(sqlPieces.get(2));
        if (groupByField == null)
            return sqlPieces.get(0) + " count(*) as rowcounts from " +
                removeOrderBy(sqlPieces.get(2));

        return sqlPieces.get(0) + " count(*) as rowcounts from (select " +
            groupByField + " from " + removeOrderBy(sqlPieces.get(2)) + ") a";
    }

    /**
     * 通过子查询来实现获取计数语句
     *
     * @param sql sql 或者 hql 语句
     * @return sql
     */
    public static String buildGetCountSQLBySubSelect(String sql) {
        List<String> sqlPieces = splitSqlByFields(sql);
        if (sqlPieces == null || sqlPieces.size() < 3)
            return "";

        if (StringUtils.isBlank(sqlPieces.get(0))) {
            sqlPieces.set(0, "select");
        }
        //这个仅仅为了兼容hibernate
        if ("from".equalsIgnoreCase(sqlPieces.get(1).trim())) {
            sqlPieces.set(1, " * from");
        }

        return sqlPieces.get(0) + " count(*) as rowCounts from (select " +
            sqlPieces.get(1) + sqlPieces.get(2) + ") a";
    }

    /**
     * sql 语句可以用 子查询和替换查询字段的方式获得总数，
     * 但是 有distinct的语句只能用子查询的方式。distinct的语句也可以用 group by的方式来转换，
     *
     * @param sql sql
     * @return sql
     */
    public static String buildGetCountSQL(String sql) {
        return buildGetCountSQLBySubSelect(sql);
    }

    /**
     * hql语句不能用子查询的方式，只能用buildGetCountSQLByReplaceFields
     *
     * @param hql sql
     * @return sql
     */
    public static String buildGetCountHQL(String hql) {
        return buildGetCountSQLByReplaceFields(hql);
    }

    /**
     * 生成PostgreSql分页查询语句
     *
     * @param sql         sql
     * @param offset      offset
     * @param maxsize     maxsize
     * @param asParameter asParameter
     * @return String
     */
    public static String buildPostgreSqlLimitQuerySQL(String sql, int offset, int maxsize, boolean asParameter) {
        if (asParameter)
            return "select * from ("+sql+") a " + (offset > 0 ? " limit ? offset ?" : " limit ?");
        else
            return "select * from ("+sql+") a " + (offset > 0 ? " limit " + maxsize + " offset " + offset :
                " limit " + maxsize);
    }

    public static String buildMySqlLimitQuerySQL(String sql, int offset, int maxsize, boolean asParameter) {
        if (asParameter)
            return "select * from ("+sql+") a " + (offset > 0 ? " limit ?, ?" : " limit ?");
        else
            return "select * from ("+sql+") a " + (offset > 0 ? " limit " + offset+ "," + maxsize:
                " limit " + maxsize);
    }

    /**
     * org.hibernate.dialect
     * 生成Oracle分页查询语句, 不考虑for update语句
     *
     * @param sql         sql
     * @param offset      offset
     * @param maxsize     maxsize
     * @param asParameter asParameter
     * @return String
     */
    public static String buildOracleLimitQuerySQL(String sql, int offset, int maxsize, boolean asParameter) {

        final StringBuilder pagingSelect = new StringBuilder(sql.length() + 100);
        if (asParameter) {
            if (offset > 0) {
                pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
            } else {
                pagingSelect.append("select * from ( ");
            }
            pagingSelect.append(sql);
            if (offset > 0) {
                pagingSelect.append(" ) row_ ) where rownum_ <= ? and rownum_ > ?");
            } else {
                pagingSelect.append(" ) where rownum <= ?");
            }
        } else {
            if (offset > 0) {
                pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
            } else {
                pagingSelect.append("select * from ( ");
            }
            pagingSelect.append(sql);
            if (offset > 0) {
                pagingSelect.append(" ) row_ ) where rownum_ <= ")
                    .append(offset + maxsize)
                    .append(" and rownum_ > ")
                    .append(offset);
            } else {
                pagingSelect.append(" ) where rownum <= ").append(maxsize);
            }
        }

        return pagingSelect.toString();
    }

    /**
     * 生成DB2分页查询语句
     *
     * @param sql     sql
     * @param offset  offset
     * @param maxsize maxsize
     * @return String
     */
    public static String buildDB2LimitQuerySQL(String sql, int offset, int maxsize/*,boolean asParameter*/)
    /*throws SQLException*/ {
        /*if(asParameter)*/
        //throw new SQLException("DB2 unsupported parameter in fetch statement.");
        if (offset == 0) {
            return maxsize > 1 ? sql + " fetch first " + maxsize + " rows only" :
                sql + " fetch first 1 row only";
        }
        //nest the main query in an outer select
        return "select * from ( select inner2_.*, rownumber() over(order by order of inner2_) as rownumber_ from ( "
            + sql + " fetch first " + String.valueOf(offset + maxsize) + " rows only ) as inner2_ ) as inner1_ where rownumber_ > "
            + offset + " order by rownumber_";
    }

    /**
     * 生成SqlServer分页查询语句
     *
     * @param sql     sql
     * @param offset  offset
     * @param maxsize maxsize
     * @return String
     */
    public static String buildSqlServerLimitQuerySQL(String sql, int offset, int maxsize/*,boolean asParameter*/){
        if (offset > 0) {

            List<String> sqlPieces = splitSqlByFields(sql);
            if (sqlPieces == null || sqlPieces.size() < 3)
                return sql;
            String alias_list = StringBaseOpt.objectToString(splitSqlFieldNames(sqlPieces.get(1)));
            String whereSql = QueryUtils.removeOrderBy(sqlPieces.get(2));
            String oderbySql = sqlPieces.get(2).substring(whereSql.length());
            if(StringUtils.isBlank(oderbySql)){
                oderbySql = "ORDER BY CURRENT_TIMESTAMP";
            } else {
                oderbySql = oderbySql.trim();
            }
            StringBuilder sqlStr = new StringBuilder(sql.length() * 2);
            sqlStr.append("WITH query AS (SELECT inner_query.* , ROW_NUMBER() OVER ( ")
                .append(oderbySql).append(" ) as __row_nr__ FROM ( ")
                .append(sqlPieces.get(0)).append(sqlPieces.get(1))
                .append(whereSql).append(") inner_query ) SELECT ")
                .append(alias_list).append( " FROM query WHERE __row_nr__ >")
                .append(offset).append(" AND __row_nr__ <= ").append(offset + maxsize);
            return sqlStr.toString();

        } else {
            Lexer sqlLexer = new Lexer(sql, Lexer.LANG_TYPE_SQL);
            StringBuilder sqlStr = new StringBuilder(sql.length() + 20);
            String sw = sqlLexer.getAWord();
            while(StringUtils.isNotBlank(sw)){
                if(sw.equals("(")){
                    int pos = sqlLexer.getCurrPos();
                    sqlLexer.seekToRightBracket();
                    int endPos = sqlLexer.getCurrPos();
                    sqlStr.append(" ").append(sql.substring(pos-1, endPos)).append(" ");
                } else if(sw.equalsIgnoreCase("select")){
                    sqlStr.append(sw).append(" ");
                    String sw2 = sqlLexer.getAWord();
                    if(sw2.equalsIgnoreCase("distinct")){
                        sqlStr.append(sw2).append(" ");
                        sw2 = sqlLexer.getAWord();
                    }
                    if(sw2.equalsIgnoreCase("top")){
                        sqlLexer.getAWord(); // 获取数字 忽略
                        sw2 = sqlLexer.getAWord();
                    }
                    sqlStr.append("top ").append(maxsize).append(" ")
                        .append(sw2).append(" ");
                } else {
                    sqlStr.append(sw).append(" ");
                }
                sw = sqlLexer.getAWord();
            }
            return sqlStr.toString();
        }
    }

    public static String buildLimitQuerySQL(String sql, int offset, int maxsize,
                                            boolean asParameter, DBType dbType) {
        switch (dbType) {
            case Oracle:
            case DM:
            case KingBase:
            case GBase:
            case Oscar:
                return buildOracleLimitQuerySQL(sql, offset, maxsize, asParameter);
            case DB2:
                return buildDB2LimitQuerySQL(sql, offset, maxsize);
            case SqlServer:
            case Access:
                return buildSqlServerLimitQuerySQL(sql, offset, maxsize);
            case MySql:
            case H2:
            case ClickHouse:
                return buildMySqlLimitQuerySQL(sql, offset, maxsize, asParameter);
            case PostgreSql:
                return buildPostgreSqlLimitQuerySQL(sql, offset, maxsize, asParameter);

            default:
                throw new ObjectException(ObjectException.ORM_METADATA_EXCEPTION,
                    "不支持的数据库类型：" + dbType);
        }
    }

    /**
     * 返回sql语句中所有的 命令变量（:变量名）,最后一个String 为转换为？变量的sql语句
     *
     * @param sql sql
     * @return 返回sql语句中所有的 命令变量（:变量名）,最后一个String 为转换为？变量的sql语句
     * Key 为转化成？的sql语句，value为对应的命名变量名，如果一个变量出现多次在list中也会出现多次
     */
    public static LeftRightPair<String, List<String>> transNamedParamSqlToParamSql(String sql) {
        StringBuilder sqlb = new StringBuilder();
        List<String> params = new ArrayList<>();
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        int prePos = 0;
        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord)) {
            if (":".equals(aWord)) {

                int curPos = lex.getCurrPos();
                if (curPos - 1 > prePos)
                    sqlb.append(sql.substring(prePos, curPos - 1));

                aWord = lex.getAWord();
                if (aWord == null || "".equals(aWord))
                    break;
                params.add(aWord);
                sqlb.append("?");
                prePos = lex.getCurrPos();
            }

            aWord = lex.getAWord();
        }
        sqlb.append(sql.substring(prePos));
        //params.add(sqlb.toString());
        return new LeftRightPair<>(sqlb.toString(), params);
    }

    /**
     * 获取sql语句中所有的 命名参数
     *
     * @param sql sql
     * @return 按照参数出现顺序排列的 list
     */
    public static List<String> getSqlNamedParameters(String sql) {
        List<String> params = new ArrayList<String>();
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord)) {
            if (":".equals(aWord)) {
                aWord = lex.getAWord();
                if (aWord == null || "".equals(aWord))
                    break;
                params.add(aWord);
            }
            aWord = lex.getAWord();
        }
        return params;
    }

    /**
     * 一、 p1.1:(like)ps return p1.1
     * 二、:(like)ps return ps
     * 三、:ps return ps
     * 四、 ps return ps
     *
     * @param pramePiece pramePiece
     * @return String
     */
    public static String fetchTemplateParamName(String pramePiece) {
        String paramName = null;
        int n = pramePiece.indexOf(':');
        if (n < 0) {//四
            return pramePiece.trim();
        }
        if (n > 1) {
            paramName = pramePiece.substring(0, n).trim();
            if (StringUtils.isNotBlank(paramName))//一
                return paramName;
        }
        //二、三
        String paramAlias = pramePiece.substring(n + 1).trim();
        n = paramAlias.lastIndexOf(')');
        if (n < 0)
            return paramAlias;
        return paramAlias.substring(n + 1).trim();
    }

    /**
     * 参数 模板 p1.1:(like,,)ps
     * 条件模板： [(条件)(参数)| 语句]
     * [参数| 语句]
     *
     * @param paramString paramString
     * @return Set String
     */
    public static Set<String> fetchTemplateParamNames(String paramString) {
        Set<String> params = new HashSet<>();
        List<String> pramePieces = Lexer.splitByWord(paramString, ",");
        for (String pramePiece : pramePieces) {
            params.add(fetchTemplateParamName(pramePiece));
        }
        return params;
    }

    /**
     * 参数 模板 p1.1:(like)ps
     * 条件模板： [(条件)(参数)| 语句]
     * [参数| 语句]
     *
     * @param queryPiece queryPiece
     * @return Set String
     */
    public static Set<String> fetchParamsFromTemplateConditions(String queryPiece) {

        Lexer varMorp = new Lexer(queryPiece, Lexer.LANG_TYPE_SQL);
        String aWord = varMorp.getARawWord();
        if (aWord == null || aWord.length() == 0)
            return null;

        Set<String> paramList = new HashSet<String>();

        if ("(".equals(aWord)) {
            //获取条件语句，如果条件语句没有，则返回 null
            int curPos = varMorp.getCurrPos();
            if (!varMorp.seekToRightBracket())
                return null;
            int prePos = varMorp.getCurrPos();
            String condition = queryPiece.substring(curPos, prePos - 1);

            Lexer labelSelected = new Lexer(condition, Lexer.LANG_TYPE_SQL);
            aWord = labelSelected.getARawWord();
            while (StringUtils.isNotBlank(aWord)) {

                if (aWord.equals("$")) {
                    aWord = labelSelected.getAWord();
                    if (aWord.equals("{")) {
                        aWord = labelSelected.getStringUntil("}");
                        paramList.add(aWord);
                    }
                } else if (Lexer.isLabel(aWord) && !VariableFormula.isKeyWord(aWord)
                    && EmbedFunc.getFuncNo(aWord) == -1) {
                    paramList.add(aWord);
                }

                aWord = labelSelected.getARawWord();
            }

            aWord = varMorp.getARawWord();
            if ("(".equals(aWord)) {
                curPos = varMorp.getCurrPos();
                if (!varMorp.seekToRightBracket())
                    return null;
                prePos = varMorp.getCurrPos();
                aWord = varMorp.getARawWord();
                String paramsString = null;
                if (prePos - 1 > curPos)
                    paramsString = queryPiece.substring(curPos, prePos - 1);
                if (paramsString != null) {//找出所有的 变量，如果变量表中没有则设置为 null
                    paramList.addAll(fetchTemplateParamNames(paramsString));
                }
            }
        } else { // 简易写法  ([:]params)* | queryPiece
            if (!varMorp.seekTo("|", false))
                return null;

            int curPos = varMorp.getCurrPos();
            String paramsString = queryPiece.substring(0, curPos - 1);
            if (StringUtils.isBlank(paramsString))
                return null;
            paramList.addAll(fetchTemplateParamNames(paramsString));
        }

        return paramList;
    }

    /*
     * 返回SqlTemplate(sql语句模板)中所有的 命令变量（:变量名）
     *  包括 [(${p1.1} &gt; 2 && p2 &gt; 2)| table1 t1,]
     *      [p1.1,:p2,p3:px| and (t2.b &gt; :p2 or t3.c &gt; :px)]
     *  中的原始参数 p1.1,p2,p3
     * @param sql sql
     * @return 返回sql语句中所有的 命令变量（:变量名）
     */
    public static Set<String> getSqlTemplateParameters(String sql) {

        Set<String> params = new HashSet<String>();
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);

        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord)) {
            if (":".equals(aWord)) {
                aWord = lex.getAWord();
                if (aWord == null || "".equals(aWord))
                    return params;
                params.add(aWord);

            } else if (aWord.equals("[")) {
                int beginPos = lex.getCurrPos();

                lex.seekToRightSquareBracket();
                int endPos = lex.getCurrPos();
                //分析表别名， 格式为 TableNameOrClass:alias,TableNameOrClass:alias,.....
                String queryPiece = sql.substring(beginPos, endPos - 1).trim();
                Set<String> subParams = fetchParamsFromTemplateConditions(queryPiece);
                if (subParams != null && subParams.size() > 0)
                    params.addAll(subParams);
            }
            aWord = lex.getAWord();
        }
        return params;
    }

    /**
     * 返回sql语句中所有的 字段 语句表达式
     * 获得查询语句中的所有 字段描述 ,比如 select a, (b+c) as d, f fn from ta 语句 返回 [ a, (b+c) as d , f fn ]
     *
     * @param sql sql
     * @return 返回feild字句，这个用户 sql语句编辑界面，在dde，stat项目中使用，一般用不到。
     */
    public static List<Pair<String, String>> extraSqlFieldNamePieceMap(String sql) {

        List<String> sqlPieces = splitSqlByFields(sql);
        if (sqlPieces == null || sqlPieces.size() < 3)
            return new ArrayList<>(0);

        return extraFieldNamePieceMap(sqlPieces.get(1));
    }

    public static Map<String, String> extraTables(String fromSql) {
        Lexer lex = new Lexer(fromSql, Lexer.LANG_TYPE_SQL);
        String aWord = lex.getAWord();
        Map<String, String> tableNameMap = new HashMap<>(4);
        while (aWord != null && !"".equals(aWord) && !StringUtils.equalsAnyIgnoreCase(aWord,
            "where", "group", "order")) {

            if("(".equals(aWord)) {
                int nPos2 = lex.getCurrPos();
                lex.seekToRightBracket();
                int nPosEnd = lex.getCurrPos();
                ;
                String subQuery = fromSql.substring(nPos2, nPosEnd);
                List<String> sqlPieces = splitSqlByFields(subQuery);
                // 子查询的别名
                aWord = lex.getAWord();
                if (sqlPieces != null && sqlPieces.size() > 2) {
                    Map<String, String> subTtableNameMap = extraTables(sqlPieces.get(2));
                    if (subTtableNameMap != null) {
                        if (!StringUtils.equalsAnyIgnoreCase(aWord,
                            ",", "left", "right", "inner", "outer", "join")) {
                            for (String subTableName : subTtableNameMap.keySet()) {
                                tableNameMap.put(subTableName, aWord);
                            }
                            aWord = lex.getAWord();
                        } else {
                            tableNameMap.putAll(subTtableNameMap);
                        }
                    } else {
                        if (!StringUtils.equalsAnyIgnoreCase(aWord,
                            ",", "left", "right", "inner", "outer", "join")) {
                            aWord = lex.getAWord();
                        }
                    }
                }
            }else {
                String talbeName = aWord;
                aWord = lex.getAWord(); // 别名
                if(!StringUtils.equalsAnyIgnoreCase(aWord,
                    ",", "on", "left", "right", "inner", "outer", "join", "where", "group", "order")){
                    tableNameMap.put(talbeName, aWord);
                    aWord = lex.getAWord();
                } else {
                    tableNameMap.put(talbeName, talbeName);
                }
            }
            if("on".equals(aWord)){
                aWord = lex.getAWord();
                while(!StringUtils.equalsAnyIgnoreCase(aWord,
                    ",", "left", "right", "inner", "outer", "join")){
                    aWord = lex.getAWord();
                }
            }
            while(StringUtils.equalsAnyIgnoreCase(aWord,
                ",", "left", "right", "inner", "outer", "join")){
                aWord = lex.getAWord();
            }
        }
        return tableNameMap;
    }
    public static Pair<List<Pair<String, String>>, Map<String, String>> extraFieldAndTable(String sql) {

        List<String> sqlPieces = splitSqlByFields(sql);
        if (sqlPieces == null || sqlPieces.size() < 3)
            return null;

        List<Pair<String, String>> fieldNameMap = extraFieldNamePieceMap(sqlPieces.get(1));
        Map<String, String> tableNameMap = extraTables(sqlPieces.get(2));

        return new MutablePair<>(fieldNameMap, tableNameMap);
    }

    public static List<Pair<String, String>> extraFieldNamePieceMap(String sFieldSql) {
        List<Pair<String, String>> fields = new ArrayList<>(20);
        Lexer lex = new Lexer(sFieldSql, Lexer.LANG_TYPE_SQL);
        int nFiledNo = 0;
        int nPos = 0;
        String aWord = lex.getAWord();

        while (aWord != null && !"".equals(aWord) && !"from".equalsIgnoreCase(aWord)) {
            int nPos2 = lex.getCurrPos();
            int nPosEnd = -1;
            String filedName = null;
            boolean prewordIsOpt = false;
            while ((!"".equals(aWord) &&
                !",".equals(aWord) &&
                !"from".equalsIgnoreCase(aWord))) {
                if ("(".equals(aWord)) {
                    lex.seekToRightBracket();
                    prewordIsOpt = false;
                } else {
                    if ("as".equalsIgnoreCase(aWord)) {
                        nPosEnd = nPos2;
                        aWord = lex.getAWord();
                        filedName = aWord;
                    } else {
                        if (Lexer.isLabel(aWord)) {
                            if (!prewordIsOpt) {
                                nPosEnd = nPos2;
                                filedName = aWord;
                            }
                            prewordIsOpt = false;
                        } else {
                            prewordIsOpt = VariableFormula.getOptID(aWord) > 0;
                            if (prewordIsOpt) {
                                filedName = null;
                            }
                        }
                    }
                }
                nPos2 = lex.getCurrPos();
                aWord = lex.getAWord();
            }

            nFiledNo++;
            if (filedName == null) {
                filedName = "column" + String.valueOf(nFiledNo);
                nPosEnd = -1;
            } else {
                /*if(filedName.endsWith("*"))
                    return null;*/
                int n = filedName.lastIndexOf('.');
                if (n > 0) {
                    filedName = filedName.substring(n + 1);
                }
            }
            fields.add(new MutablePair<>(
                filedName, sFieldSql.substring(nPos, (nPosEnd > nPos ? nPosEnd : nPos2)).trim()));

            nPos = nPos2;
            if (",".equals(aWord)) {
                nPos = lex.getCurrPos();
                aWord = lex.getAWord();
                //filedName = aWord;
            }
        }

        return fields;
    }

    /**
     * 返回sql语句中所有的 字段 语句表达式
     * 获得查询语句中的所有 字段描述 ,比如 select a, (b+c) as d, f fn from ta 语句 返回 [ a, (b+c) as d , f fn ]
     *
     * @param sql sql
     * @return 返回feild字句，这个用户 sql语句编辑界面，在dde，stat项目中使用，一般用不到。
     */
    public static List<String> getSqlFieldPieces(String sql) {

        List<String> fields = new ArrayList<>(5);
        List<String> sqlPieces = splitSqlByFields(sql);
        if (sqlPieces == null || sqlPieces.size() < 3)
            return fields;

        String sFieldSql = sqlPieces.get(1);
        Lexer lex = new Lexer(sFieldSql, Lexer.LANG_TYPE_SQL);

        int nPos = 0;
        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord) && !"from".equalsIgnoreCase(aWord)) {
            int nPos2 = lex.getCurrPos();
            while (!"".equals(aWord) && !",".equals(aWord) && !"from".equalsIgnoreCase(aWord)) {
                if ("(".equals(aWord)) {
                    lex.seekToRightBracket();
                }
                nPos2 = lex.getCurrPos();
                aWord = lex.getAWord();
            }

            fields.add(sFieldSql.substring(nPos, nPos2).trim());
            nPos = nPos2;
            if (",".equals(aWord)) {
                nPos = lex.getCurrPos();
                aWord = lex.getAWord();
            }
        }
        return fields;
    }

    /**
     * 返回sql语句中所有的 字段 名称
     * 获得 查询语句中的所有 字段名称,比如   a, (b+c) as d, f fn from 语句 返回 [a,d,fn]
     *
     * @param sFieldSql sFieldSql
     * @return 字段名子列表
     */
    public static List<String> splitSqlFieldNames(String sFieldSql) {
        List<String> fields = new ArrayList<>(20);
        Lexer lex = new Lexer(sFieldSql, Lexer.LANG_TYPE_SQL);

        String aWord = lex.getAWord();
        String filedName = aWord;
        int nFiledNo = 0;
        while (aWord != null && !"".equals(aWord) && !"from".equalsIgnoreCase(aWord)) {
            while (!"".equals(aWord) && !",".equals(aWord)
                && !"from".equalsIgnoreCase(aWord)) {
                if ("(".equals(aWord)) {
                    lex.seekToRightBracket();
                    filedName = null;
                } else {
                    // 如果有 * 则不能解析 字段名
                    if ("*".equals(aWord)) {
                        return null;
                    }
                    if (VariableFormula.getOptID(aWord) > 0) {
                        filedName = null;
                    } else {
                        filedName = StringRegularOpt.trimString(aWord);
                    }
                }
                aWord = lex.getAWord();
            }

            nFiledNo++;

            if (filedName == null) {
                filedName = "";
            } else {
                /*if(filedName.endsWith("*"))
                    return null;*/
                int n = filedName.lastIndexOf('.');
                if (n > 0) {
                    filedName = filedName.substring(n + 1);
                    // 如果有 * 则不能解析 字段名
                    if ("*".equals(filedName)) {
                        return null;
                    }
                }
            }

            fields.add(filedName);
            if (",".equals(aWord)) {
                filedName = aWord;
                aWord = lex.getAWord();
            }
        }
        return fields;
    }

    /**
     * 返回sql语句中所有的 字段 名称
     * 获得 查询语句中的所有 字段名称,比如 select a, (b+c) as d, f fn from ta 语句 返回 [a,d,fn]
     *
     * @param sql sql
     * @return 字段名子列表 ，  如果 查询语句中有 * 将返回  null
     */
    public static List<String> getSqlFiledNames(String sql) {
        List<String> sqlPieces = splitSqlByFields(sql);
        if (sqlPieces == null || sqlPieces.size() < 3)
            return null;
        return splitSqlFieldNames(sqlPieces.get(1));
    }

    /**
     * 返回SqlTemplate(sql语句模板)中所有的所有的 字段 名称
     * 获得 查询语句中的所有 字段名称,比如 select a, (b+c) as d, f fn from ta 语句 返回 [a,d,fn]
     *
     * @param sql sql
     * @return 字段名子列表
     */
    public static List<String> getSqlTemplateFiledNames(String sql) {
        List<String> sqlPieces = splitSqlByFields(sql);
        if (sqlPieces == null || sqlPieces.size() < 3)
            return null;

        String sFieldSql = sqlPieces.get(1);
        Lexer varMorp = new Lexer(sFieldSql, Lexer.LANG_TYPE_SQL);
        StringBuilder sbSql = new StringBuilder();
        int prePos = 0;
        String aWord = varMorp.getAWord();
        while (aWord != null && !"".equals(aWord) && !"from".equalsIgnoreCase(aWord)) {
            if (aWord.equals("[")) {
                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    sbSql.append(sFieldSql.substring(prePos, curPos - 1));

                aWord = varMorp.getAWord();
                while (aWord != null && !"|".equals(aWord)) {
                    if ("(".equals(aWord)) {
                        varMorp.seekToRightBracket();
                    }
                    aWord = varMorp.getAWord();
                }
                if ("|".equals(aWord)) {
                    curPos = varMorp.getCurrPos();
                    varMorp.seekToRightSquareBracket();
                    prePos = varMorp.getCurrPos();
                    sbSql.append(sFieldSql.substring(curPos, prePos - 1));
                }
                aWord = varMorp.getAWord();
            }
            aWord = varMorp.getAWord();
        }
        sbSql.append(sFieldSql.substring(prePos));

        return splitSqlFieldNames(sbSql.toString());
    }

    /**
     * 过滤 order by 语句中无效信息，在可能带入乱码和注入的情况下使用
     *
     * @param sqlOrderBy sqlOrderBy
     * @return String
     */
    public static String trimSqlOrderByField(String sqlOrderBy) {
        if (sqlOrderBy == null)
            return null;

        StringBuilder sb = new StringBuilder();

        Lexer lex = new Lexer(sqlOrderBy, Lexer.LANG_TYPE_SQL);
        boolean haveOrder = false;
        boolean bLastDouHao = false;
        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord)) {
            if (Lexer.isLabel(aWord) || StringUtils.isNumeric(aWord)) {
                if (haveOrder) {
                    if (bLastDouHao)
                        sb.append(",");
                    else
                        return null;
                }
                sb.append(aWord);
                haveOrder = true;
                bLastDouHao = false;
                aWord = lex.getAWord();
                if ("asc".equalsIgnoreCase(aWord) || "desc".equalsIgnoreCase(aWord)) {
                    sb.append(" ").append(aWord);
                    aWord = lex.getAWord();
                }
                if ("nulls".equalsIgnoreCase(aWord)) {
                    aWord = lex.getAWord();
                    if ("first".equalsIgnoreCase(aWord) || "last".equalsIgnoreCase(aWord)) {
                        sb.append(" nulls ").append(aWord);
                        aWord = lex.getAWord();
                    } else
                        return null;
                }
            } else if (",".equals(aWord)) {
                if (bLastDouHao)
                    return null;
                bLastDouHao = true;//sb.append(",");
                aWord = lex.getAWord();
            } else
                return null;
        }

        return sb.toString();
    }

    /**
     * 创建sql语句参数键值对
     *
     * @param objs 奇数变量为参数名，类型为string，偶数变量为参数值，类型为任意对象（object）
     * @return Map String Object
     * @see com.centit.support.algorithm.CollectionsOpt 方法 createHashMap
     */
    @Deprecated
    public static Map<String, Object> createSqlParamsMap(Object... objs) {
        return CollectionsOpt.createHashMap(objs);
    }

    /**
     * 对参数进行预处理
     *
     * @param pretreatment pretreatment
     * @param paramValue   paramValue
     * @return Object
     */
    public static Object scalarPretreatParameter(String pretreatment, Object paramValue) {
        if (paramValue == null)
            return null;
        switch (pretreatment.toUpperCase()) {
            case SQL_PRETREAT_LIKE:
                return getMatchString(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_STARTWITH:
                return StringBaseOpt.objectToString(paramValue) + "%";
            case SQL_PRETREAT_ENDWITH:
                return "%" + StringBaseOpt.objectToString(paramValue);
            case SQL_PRETREAT_NEXT_DAY:
                return DatetimeOpt.addDays(DatetimeOpt.truncateToDay(
                    DatetimeOpt.castObjectToDate(paramValue)), 1);
            case SQL_PRETREAT_NEXT_MONTH:
                return DatetimeOpt.addMonths(DatetimeOpt.truncateToMonth(
                    DatetimeOpt.castObjectToDate(paramValue)), 1);
            case SQL_PRETREAT_NEXT_YEAR:
                return DatetimeOpt.addYears(DatetimeOpt.truncateToYear(
                    DatetimeOpt.castObjectToDate(paramValue)), 1);
            case SQL_PRETREAT_NEXT_WEEK:
                return DatetimeOpt.addDays(DatetimeOpt.truncateToWeek(
                    DatetimeOpt.castObjectToDate(paramValue)), 7);

            case SQL_PRETREAT_TRUNC_MONTH:
                return DatetimeOpt.truncateToMonth(
                    DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_TRUNC_YEAR:
                return DatetimeOpt.truncateToYear(
                    DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_TRUNC_WEEK:
                return DatetimeOpt.truncateToWeek(
                    DatetimeOpt.castObjectToDate(paramValue));

            case SQL_PRETREAT_TRUNC_DAY:
            case SQL_PRETREAT_DATE:
                return DatetimeOpt.truncateToDay(
                    DatetimeOpt.castObjectToDate(paramValue));

            case SQL_PRETREAT_DATETIME:
                return DatetimeOpt.castObjectToDate(paramValue);

            case SQL_PRETREAT_DATESTR:
                return DatetimeOpt.convertDateToString(
                    DatetimeOpt.castObjectToDate(paramValue));

            case SQL_PRETREAT_DATETIMESTR:
                return DatetimeOpt.convertDatetimeToString(
                    DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_DIGIT:
                return StringRegularOpt.trimDigits(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_UPPERCASE:
                return StringUtils.upperCase(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_LOWERCASE:
                return StringUtils.lowerCase(StringBaseOpt.objectToString(paramValue));

            case SQL_PRETREAT_NUMBER:
                return StringRegularOpt.trimNumber(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_INTEGER:
            case SQL_PRETREAT_LONG:
                return NumberBaseOpt.castObjectToLong(paramValue);
            case SQL_PRETREAT_FLOAT:
                return NumberBaseOpt.castObjectToDouble(paramValue);
            case SQL_PRETREAT_ESCAPE_HTML:
                return StringEscapeUtils.escapeHtml4(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_QUOTASTR:
                return buildStringForQuery(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_MAPTOFIELD:
                return FieldType.mapPropName(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_MAP_NAME_COLUMN:
                return FieldType.humpNameToColumn(StringBaseOpt.objectToString(paramValue), true);
            case SQL_PRETREAT_STRING:
                return StringBaseOpt.objectToString(paramValue);

            default:
                return paramValue;
        }
    }

    public static Object onePretreatParameter(String pretreatment, Object paramValue) {
        if (paramValue == null)
            return null;
        if (SQL_PRETREAT_STRING.equalsIgnoreCase(pretreatment))
            return StringBaseOpt.objectToString(paramValue);
        if (SQL_PRETREAT_SPLITFORIN.equalsIgnoreCase(pretreatment)) {
            String sValue = StringBaseOpt.objectToString(paramValue);
            if (sValue == null)
                return null;
            if(sValue.indexOf(',')>0)
                return sValue.split(",");
            if(sValue.indexOf('+')>0)
                return sValue.split("\\+");
            return StringUtils.split(sValue);
        }
        if (paramValue instanceof Collection) {
            Collection<?> valueList = (Collection<?>) paramValue;
            List<Object> retValue = new ArrayList<>();
            for (Object ov : valueList) {
                Object ro = scalarPretreatParameter(pretreatment, ov);
                if (ro != null) {
                    retValue.add(ro);
                }
            }
            if (retValue.size() < 1)
                return null;
            return retValue;
        } else if (paramValue instanceof Object[]) {
            Object[] objs = (Object[]) paramValue;

            List<Object> retValue = new ArrayList<>();
            for (Object ov : objs) {
                Object ro = scalarPretreatParameter(pretreatment, ov);
                if (ro != null) {
                    retValue.add(ro);
                }
            }
            if (retValue.size() < 1)
                return null;
            return retValue;
        } else
            return scalarPretreatParameter(pretreatment, paramValue);
        //if(SQL_PRETREAT_CREEPFORIN.equalsIgnoreCase(pretreatment))
        //return String.valueOf(paramValue).split(",");
    }

    public static Map<String, Object> pretreatParameters(Map<String, Object> filterMap) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, Object> ent : filterMap.entrySet()) {
            String key = ent.getKey();
            Object value = ent.getValue();
            String paramAlias = key;
            int nPos = key.indexOf('(');
            int nRevPos = key.lastIndexOf(')');
            if(nPos>=0 && nRevPos>=0){
                String pretreatment = key.substring(nPos+1, nRevPos).trim();
                paramAlias = nPos > 1 ? key.substring(0,nPos).trim()
                    :key.substring(nRevPos+1).trim();
                value = QueryUtils.pretreatParameter(pretreatment, value);
            }
            map.put(paramAlias, value);
        }
        return map;
    }

    /**
     * 对参数进行预处理
     *
     * @param pretreatment, 可以有多个，用','分开
     * @param paramValue    paramString
     * @return Object
     */
    public static Object pretreatParameter(String pretreatment, Object paramValue) {
        if (StringUtils.isBlank(pretreatment) || paramValue==null)
            return paramValue;
        if (pretreatment.indexOf(',') < 0) {
            return onePretreatParameter(pretreatment, paramValue);
        }
        String[] pretreats = pretreatment.split(",");
        Object paramObj = paramValue;
        for (String p : pretreats) {
            paramObj = onePretreatParameter(p, paramObj);
        }
        return paramObj;
    }

    public static List<String> splitParamString(String paramString) {
        List<String> params = new ArrayList<>();
        Lexer lex = new Lexer(paramString, Lexer.LANG_TYPE_SQL);
        int prePos = 0;
        String aWord = lex.getAWord();
        while (aWord != null && !"".equals(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
            } else if (aWord.equals(",")) {
                int currPos = lex.getCurrPos();
                params.add(paramString.substring(prePos, currPos - 1));
                prePos = currPos;
            }
            aWord = lex.getAWord();
        }
        if (prePos < paramString.length())
            params.add(paramString.substring(prePos));
        return params;
    }

    /**
     * 参数表示式的完整形式是  :  表达式：(预处理,预处理2,......)参数名称
     *
     * @param paramString paramString
     * @return 返回为Triple "表达式","参数名称","预处理,预处理2,......"
     */
    public static ImmutableTriple<String, String, String> parseParameter(String paramString) {
        /*if(StringUtils.isBlank(paramString))
            return null;*/
        String paramName;
        String paramRight;
        String paramPretreatment = null;
        String paramAlias = null;
        int n = paramString.indexOf(':');
        if (n >= 0) {
            paramRight = paramString.substring(n + 1).trim();
            if (paramRight.charAt(0) == '(') {
                int e = paramRight.indexOf(')');
                if (e > 0) {
                    paramPretreatment = paramRight.substring(1, e).trim();
                    paramAlias = paramRight.substring(e + 1).trim();
                }
            } else
                paramAlias = paramRight;

            if (n > 1) {
                paramName = paramString.substring(0, n).trim();
            } else
                paramName = paramAlias;
        } else {
            int e = paramString.indexOf(')');
            if (e > 0) {
                int b = paramString.indexOf('(');
                paramPretreatment = paramString.substring(b + 1, e).trim();
                if(paramString.length() > e + 1)
                    paramName = paramString.substring(e + 1).trim();
                else if(b>0){
                    paramName = paramString.substring(0, b).trim();
                } else {
                    paramName = null;
                }
            } else
                paramName = paramString;
        }
        return new ImmutableTriple<>(paramName, paramAlias, paramPretreatment);
    }

    /**
     * 通过参数数组 编译重复语句
     *
     * @param paramAlias 参数别名
     * @param realParam  参数实际值
     * @return sql语句和参数列表
     */
    public static QueryAndNamedParams buildInStatement(String paramAlias, Object realParam) {
        StringBuilder hqlPiece = new StringBuilder();
        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();

        //hqlPiece.append("(");
        if (realParam instanceof Collection) {
            int n = 0;
            for (Object obj : (Collection<?>) realParam) {
                if (n > 0)
                    hqlPiece.append(",");
                hqlPiece.append(":").append(paramAlias).append('_').append(n);
                hqlAndParams.addParam(paramAlias + "_" + n, obj);
                n++;
            }
        } else if (realParam instanceof Object[]) {
            int n = 0;
            for (Object obj : (Object[]) realParam) {
                if (n > 0)
                    hqlPiece.append(",");
                hqlPiece.append(":").append(paramAlias).append('_').append(n);
                hqlAndParams.addParam(paramAlias + "_" + n, obj);
                n++;
            }
        } else {
            hqlPiece.append(":").append(paramAlias);
            hqlAndParams.addParam(paramAlias, realParam);
        }
        //hqlPiece.append(")");
        hqlAndParams.setQuery(hqlPiece.toString());
        return hqlAndParams;
    }

    /**
     * 整理sql的标识符，避免sql注入， 只保留 数字，字母，下划线和 小数点
     * @param identifier 标识符
     * @return 标识符
     */
    public static String trimSqlIdentifier(String identifier) {
        if (StringUtils.isBlank(identifier))
            return "";
        StringBuilder sbNew = new StringBuilder(identifier.length());
        for(int i=0; i<identifier.length(); i++){
            char c = identifier.charAt(i);
            if( (c>='a' && c <='z') || (c>='A' && c <='Z')
                || (c>='0' && c <= '9')
                || c=='_' || c == '.' || c=='#' || c == '@'){
                sbNew.append(c);
            }
        }
        return sbNew.toString();
    }

    /**
     * 判断是否sql的标识符，避免sql注入， 只保留 数字，字母，下划线和 小数点
     * @param identifier 标识符
     * @return 是否是标识符
     */
    public static boolean checkSqlIdentifier(String identifier) {
        if (StringUtils.isBlank(identifier))
            return false;
        for(int i=0; i<identifier.length(); i++){
            char c = identifier.charAt(i);
            if( (c<'a' || c >'z') && (c<'A' || c >'Z')
                && (c<'0' || c >'9')
                && c!='_' && c != '.' && c!='#' && c != '@' ){
                return false;
            }
        }
        return true;
    }

    /**
     * 去掉 分号 ； 和  单行注释   / * 注释保留 * /
     * 用于 in-place 操作指令，不符合规范直接抛出异常
     * @param fieldsSql paramString
     * @return String
     */
    public static String cleanSqlStatement(String fieldsSql) {
        if (StringUtils.isBlank(fieldsSql))
            return fieldsSql;
        Lexer lex = new Lexer(fieldsSql, Lexer.LANG_TYPE_SQL);
        StringBuilder fieldSb = new StringBuilder();

        String aWord = lex.getAWord();
        int pos = 0;
        while (StringUtils.isNotBlank(aWord)) {
            if (StringUtils.equalsAnyIgnoreCase(aWord,
                ";", "select", "delete", "update", "insert", "into", "from", "where",
                            "truncate", "drop", "create", "alter", "merge",
                            "grant", "revoke", "explain", "transaction")) {
                throw new ObjectException(ObjectException.DATA_VALIDATE_ERROR,
                    "非法的SQL参数："+ fieldsSql);
            }
            if(pos>0) fieldSb.append(' ');
            fieldSb.append(aWord);
            pos ++;
            aWord = lex.getAWord();
        }
        return fieldSb.toString();
    }

    public static String replaceParamAsSqlString(String sql, String paramAlias, String paramSqlString) {
        Lexer varMorp = new Lexer(sql, Lexer.LANG_TYPE_SQL);

        String sWord = varMorp.getAWord();
        while (sWord != null && !sWord.equals("")) {
            if (":".equals(sWord)) {
                int prePos = varMorp.getCurrPos();
                sWord = varMorp.getAWord();
                if (paramAlias.equals(sWord)) {
                    int curPos = varMorp.getCurrPos();
                    String resSql = "";
                    if (prePos > 1)
                        resSql = sql.substring(0, prePos - 1);
                    resSql = resSql + paramSqlString;
                    if (curPos < sql.length())
                        resSql = resSql + sql.substring(curPos);
                    return resSql;
                }
            }
            sWord = varMorp.getAWord();
        }
        return sql;
    }

    public static boolean hasPretreatment(String pretreatStr, String onePretreat) {
        if (pretreatStr == null) return false;
        return pretreatStr.toUpperCase().contains(onePretreat);
    }

    /**
     * @param filter     转换为 sql
     * @param translater 变量内嵌在语句中，不用参数
     * @return QueryAndNamedParams
     */
    public static QueryAndNamedParams translateQueryFilter(String filter, IFilterTranslater translater) {
        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();
        Lexer varMorp = new Lexer(filter, Lexer.LANG_TYPE_SQL);
        StringBuilder hqlPiece = new StringBuilder();
        String sWord = varMorp.getAWord();
        int prePos = 0;
        while (sWord != null && !sWord.equals("")) {
            if (sWord.equals("[")) {
                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    hqlPiece.append(filter.substring(prePos, curPos - 1));
                varMorp.seekToRightSquareBracket();//.seekTo(']');
                prePos = varMorp.getCurrPos();
                String columnDesc = filter.substring(curPos, prePos - 1).trim();

                String qp = translater.translateColumn(columnDesc);
                if (qp == null)
                    return null;

                hqlPiece.append(qp);

            } else if (sWord.equals("{")) {
                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    hqlPiece.append(filter.substring(prePos, curPos - 1));
                varMorp.seekToRightBrace();//('}');
                prePos = varMorp.getCurrPos();
                if(prePos <= curPos+1){
                    return null; // 变量名为空，格式不正确
                }
                String param = filter.substring(curPos, prePos - 1).trim();
                if (StringUtils.isBlank(param)) {
                    return null; // 变量名为空，格式不正确
                }
                ImmutableTriple<String, String, String> paramMeta = parseParameter(param);
                //{paramName,paramAlias,paramPretreatment};
                String paramName = StringUtils.isBlank(paramMeta.left) ? paramMeta.middle : paramMeta.left;
                String paramAlias = StringUtils.isBlank(paramMeta.middle) ? paramMeta.left : paramMeta.middle;

                LeftRightPair<String, Object> paramPair = translater.translateParam(paramName);
                if (paramPair == null)
                    return null;

                if (paramPair.getRight() != null) {
                    Object realParam = pretreatParameter(paramMeta.right, paramPair.getRight());
                    if (hasPretreatment(paramMeta.right, SQL_PRETREAT_CREEPFORIN)) {
                        QueryAndNamedParams inSt = buildInStatement(paramAlias, realParam);
                        hqlPiece.append(inSt.getQuery());
                        hqlAndParams.addAllParams(inSt.getParams());
                    } else if (hasPretreatment(paramMeta.right, SQL_PRETREAT_INPLACE)) {
                        hqlPiece.append(cleanSqlStatement(StringBaseOpt.objectToString(realParam)));
                    } else {
                        hqlPiece.append(":").append(paramAlias);
                        hqlAndParams.addParam(paramAlias, realParam);
                    }

                } else {
                    hqlPiece.append(paramPair.getLeft());
                }
            }

            sWord = varMorp.getAWord();
        }
        hqlPiece.append(filter.substring(prePos));
        hqlAndParams.setQuery(hqlPiece.toString());
        if(StringUtils.isBlank(hqlAndParams.getQuery())){
            return null;
        }
        return hqlAndParams;
    }

    public static QueryAndNamedParams translateQueryFilter(Collection<String> filters,
                                                           IFilterTranslater translater, boolean isUnion) {
        if (filters == null || filters.isEmpty())
            return null;
        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();
        StringBuilder hqlBuilder = new StringBuilder();

        int hqlPieceCount = 0;
        for (String filter : filters) {
            QueryAndNamedParams hqlPiece = translateQueryFilter(filter, translater);
            if (hqlPiece != null && StringUtils.isNotBlank(hqlPiece.getQuery())) {
                if (hqlPieceCount>0)
                    hqlBuilder.append(isUnion ? " or " : " and ");
                hqlPieceCount ++;
                hqlBuilder.append(hqlPiece.getQuery());
                hqlAndParams.addAllParams(hqlPiece.getParams());
            }
        }

        if (hqlPieceCount == 0)
            return null;
        if(hqlPieceCount > 1 && isUnion){
            hqlAndParams.setQuery("( " + hqlBuilder.toString() + " )");
        } else {
            hqlAndParams.setQuery(hqlBuilder.toString());
        }
        return hqlAndParams;
    }

    private static void sqlCreepByValue(QueryAndNamedParams hqlAndParams, String paramPretreat, String paramAlias, Object realParam) {
        String sql = hqlAndParams.getQuery();
        if (hasPretreatment(paramPretreat, SQL_PRETREAT_CREEPFORIN)) {
            QueryAndNamedParams inSt = buildInStatement(paramAlias, realParam);
            hqlAndParams.addAllParams(inSt.getParams());
            sql = replaceParamAsSqlString(
                sql, paramAlias, inSt.getQuery());
            hqlAndParams.setQuery(sql);
        } else if (hasPretreatment(paramPretreat, SQL_PRETREAT_INPLACE)) {
            sql = replaceParamAsSqlString(
                sql, paramAlias, cleanSqlStatement(StringBaseOpt.objectToString(realParam)));
            hqlAndParams.setQuery(sql);
        }  else if (hasPretreatment(paramPretreat, SQL_PRETREAT_LOOP_WITH_OR)) {
            QueryAndNamedParams inSt = buildInStatement(paramAlias, realParam);
            hqlAndParams.addAllParams(inSt.getParams());
            StringBuilder sb = new StringBuilder(" and (");
            int n =0;
            for(Map.Entry<String, Object> ent : inSt.getParams().entrySet()){
                if(n>0) sb.append(" or ");
                sb.append(replaceParamAsSqlString( sql, paramAlias,":" + ent.getKey()));
                n++;
            }
            sql = sb.append(")").toString();
            hqlAndParams.setQuery(sql);
        } else if (hasPretreatment(paramPretreat, SQL_PRETREAT_LOOP)) {
            QueryAndNamedParams inSt = buildInStatement(paramAlias, realParam);
            hqlAndParams.addAllParams(inSt.getParams());
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<String, Object> ent : inSt.getParams().entrySet()){
                sb.append(replaceParamAsSqlString( sql, paramAlias, ":" + ent.getKey()));
            }
            sql = sb.toString();
            hqlAndParams.setQuery(sql);
        } else {
            hqlAndParams.addParam(paramAlias, realParam);
        }
    }

    public static QueryAndNamedParams translateQueryPiece(
        String queryPiece, IFilterTranslater translater) {

        Lexer varMorp = new Lexer(queryPiece, Lexer.LANG_TYPE_SQL);
        String aWord = varMorp.getARawWord();
        if (aWord == null || aWord.length() == 0)
            return null;

        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();

        if ("(".equals(aWord)) {
            //获取条件语句，如果条件语句没有，则返回 null
            int curPos = varMorp.getCurrPos();
            if (!varMorp.seekToRightBracket())
                return null;
            int prePos = varMorp.getCurrPos();
            String condition = queryPiece.substring(curPos, prePos - 1);

            Object sret = VariableFormula.calculate(condition, translater);
            if (!BooleanBaseOpt.castObjectToBoolean(sret, false))
                return null;

            String paramsString = null;
            aWord = varMorp.getARawWord();

            if ("(".equals(aWord)) {
                curPos = varMorp.getCurrPos();
                if (!varMorp.seekToRightBracket())
                    return null;
                prePos = varMorp.getCurrPos();
                if (prePos - 1 > curPos)
                    paramsString = queryPiece.substring(curPos, prePos - 1);
                aWord = varMorp.getARawWord();
            }

            if ("|".equals(aWord)) {
                prePos = varMorp.getCurrPos();
            }//按道理这里是需要报错的
            String sql = queryPiece.substring(prePos);
            if (StringUtils.isBlank(sql))
                return null;

            hqlAndParams.setQuery(sql);

            if (paramsString != null) {//找出所有的 变量，如果变量表中没有则设置为 null
                List<String> params = splitParamString(paramsString);
                //String [] params = paramsString.split(",");
                for (String param : params) {
                    if (StringUtils.isNotBlank(param)) {
                        ImmutableTriple<String, String, String> paramMeta = parseParameter(param);
                        //{paramName,paramAlias,paramPretreatment};
                        String paramName = StringUtils.isBlank(paramMeta.left) ? paramMeta.middle : paramMeta.left;
                        String paramAlias = StringUtils.isBlank(paramMeta.middle) ? paramMeta.left : paramMeta.middle;
                        LeftRightPair<String, Object> paramPair = translater.translateParam(paramName);

                        if (paramPair != null && paramPair.getRight() != null) {
                            Object realParam = pretreatParameter(paramMeta.right, paramPair.getRight());
                            sqlCreepByValue(hqlAndParams, paramMeta.right, paramAlias, realParam);
                        }
                    }
                }
            }

        } else { // 简易写法  ([:]params)* | queryPiece
            if (!varMorp.seekTo("|", false))
                return null;

            int curPos = varMorp.getCurrPos();
            String sql = queryPiece.substring(curPos);
            if (StringUtils.isBlank(sql))
                return null;

            String paramsString = queryPiece.substring(0, curPos - 1);
            if (StringUtils.isBlank(paramsString))
                return null;

            hqlAndParams.setQuery(sql);
            List<String> params = splitParamString(paramsString);
            //String [] params = paramsString.split(",");
            for (String param : params) {
                if (StringUtils.isNotBlank(param)) {
                    ImmutableTriple<String, String, String> paramMeta = parseParameter(param);
                    //{paramName,paramAlias,paramPretreatment};
                    boolean addParams = !StringUtils.isBlank(paramMeta.middle);
                    String paramName = StringUtils.isBlank(paramMeta.left) ? paramMeta.middle : paramMeta.left;
                    String paramAlias = addParams ? paramMeta.middle : paramMeta.left;

                    LeftRightPair<String, Object> paramPair = translater.translateParam(paramName);
                    if (paramPair == null || paramPair.getRight() == null)
                        return null;
                    if (addParams) {
                        Object realParam = pretreatParameter(paramMeta.right, paramPair.getRight());
                        sqlCreepByValue(hqlAndParams, paramMeta.right, paramAlias, realParam);
                    }
                }
            }//end of for

        }
        return hqlAndParams;
    }

    public static QueryAndNamedParams translateQuery(
        String queryStatement, Collection<String> filters,
        boolean isUnion, IFilterTranslater translater) {

        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();
        Lexer varMorp = new Lexer(queryStatement, Lexer.LANG_TYPE_SQL);
        StringBuilder hqlBuilder = new StringBuilder();
        String sWord = varMorp.getAWord();
        int prePos = 0;
        while (sWord != null && !sWord.equals("")) {
            if (sWord.equals("{")) {

                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    hqlBuilder.append(queryStatement.substring(prePos, curPos - 1));
                varMorp.seekToRightBrace();//.seekTo('}');
                prePos = varMorp.getCurrPos();
                //分析表别名， 格式为 TableNameOrClass:alias,TableNameOrClass:alias,.....
                String tablesDesc = queryStatement.substring(curPos, prePos - 1).trim();
                //required 关键字表示必须有对应的权限过滤语句，如果没有 则恒为false
                boolean required = false;
                String firstWord = Lexer.getFirstWord(tablesDesc);
                if("required".equalsIgnoreCase(firstWord)){
                    required = true;
                    tablesDesc = tablesDesc.substring(8).trim();
                }

                String[] tables = tablesDesc.split(",");
                Map<String, String> tableMap = new HashMap<>();
                for (String tableDesc : tables) {
                    Lexer tableLexer = new Lexer(tableDesc, Lexer.LANG_TYPE_SQL);
                    String tableName = tableLexer.getAWord();
                    String aliasName = tableLexer.getAWord();
                    if (":".equals(aliasName)) {
                        aliasName = tableLexer.getAWord();
                    }
                    tableMap.put(tableName, aliasName);
                }
                translater.setTableAlias(tableMap);
                QueryAndNamedParams hqlPiece =
                    translateQueryFilter(filters,
                        translater, isUnion);

                if (hqlPiece != null && !StringBaseOpt.isNvl(hqlPiece.getQuery())) {
                    hqlBuilder.append(" and ").append(hqlPiece.getQuery());
                    hqlAndParams.addAllParams(hqlPiece.getParams());
                } else if(required){
                    //必须要有范围权限，否则就添加永远是false的语句
                    hqlBuilder.append(" and 0=1 ");
                }

            } else if (sWord.equals("[")) {
                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    hqlBuilder.append(queryStatement.substring(prePos, curPos - 1));
                varMorp.seekToRightSquareBracket();
                prePos = varMorp.getCurrPos();
                //分析表别名， 格式为 TableNameOrClass:alias,TableNameOrClass:alias,.....
                String queryPiece = queryStatement.substring(curPos, prePos - 1).trim();

                QueryAndNamedParams hqlPiece =
                    translateQueryPiece(queryPiece, translater);

                if (hqlPiece != null && StringUtils.isNotBlank(hqlPiece.getQuery())) {
                    hqlBuilder.append(hqlPiece.getQuery());
                    hqlAndParams.addAllParams(hqlPiece.getParams());
                }
            }
            sWord = varMorp.getAWord();
        }
        hqlBuilder.append(queryStatement.substring(prePos));
        hqlAndParams.setQuery(hqlBuilder.toString());
        return hqlAndParams;
    }

    /*
     * 这个函数是为了满足 根据前端查询表单中的参数填写情况动态拼接查询语句条件的的需求而设计的。
     * 传统的办法是用if语句一个一个的判断，这样是可以工作的，但是这样query语句非常零碎，容易出错。
     *
     * 这个函数中包括了两种拼接query的方法：
     * 方法一： 过滤语句filter外置
     *         1,用一个Collection 《String》类存放所有可能的条件语句。
     *             filter过滤语句为一个 逻辑语句，其中用[filterName.field]来标识字段或者hql的属性
     *             用{paramName:(pretreat)paramAlias}来标识变量,前面的paramName标识变量名，解释器通过这个获取数值，
     *                 后面的paramAlias标识加入到最后查询中参数名称，如果两个一样可以简写成{paramName}。
     *                 在权限引擎中变量名paramName是当前用户的相关属性，这个变量名可能比较复杂并且用'.'来表示层级关系
     *                 (pretreat) 这个是可选的， pretreat
     *                 所以需要重命名后加入到最终查询中，这样便于理解。这个格式和方法二中的变量保存一致,详细写法参见方法二
     *         2,在语句queryStatement中 {filterName:alias,filterName2:alias,....]}来标识语句占位符。
     *             它只能出现在where语句部分，如果语句中有子查询在子查询的where部分也可以使用。
     *         3,isUnion是在同一个占位符中有多个符合条件的过滤语句时之间的拼接方式，true用Or拼接，false用and拼接。
     *     函数根据filters值、语句中的占位符和查询变量来决定占位符替换的内容。举个列子：
                 List《String》 filters = new ArrayList《String》 ();
                filters.add("[table1.c] like {p1.1:ps}");                       (1)
                filters.add("[table1.b] = {p5}");                          (2)
                filters.add("[table4.b] = {p4}");                          (3)
                filters.add("([table2.f]={p2} and [table3.f]={p3})");      (4)

                Map《String,Object》 paramsMap = new HashMap《String,Object》();
                paramsMap.put("p1.1", "1");
                paramsMap.put("p2", "3");

                String queryStatement = "select t1.a,t2.b,t3.c "+
                    "from table1 t1,table2 t2,table3 t3 "+
                    "where 1=1 {table1:t1} order by 1,2";

                System.out.println(translateQuery(queryStatement,filters,paramsMap,true).getQuery());
                结果是：
                select t1.a,t2.b,t3.c from table1 t1,table2 t2,table3 t3
                where 1=1  and (t1.c like :ps ) order by 1,2

                因为{table1:t1}只有table1所以只能选中(1)和(2),但(2)中要求参数p5在paramsMap中没有所以只能选中(1),把(1)中的
                table1替换为别名t1变量{p1}替换为:p1,同时在返回值的QueryAndNamedParams中添加变量p1。

                再看一个复杂的例子：
                queryStatement = "select t1.a,t2.b,t3.c "+
                        "from table1 t1,table2 t2,table3 t3 "+
                        "where 1=1 {table1:t1}{table9:t1}{table2:t2,table3:t3,table4:t1} order by 1,2";
                paramsMap.put("p3", "5");
                paramsMap.put("p4", "7");
                System.out.println(translateQuery(queryStatement,filters,paramsMap,true).getQuery());
                结果是：
                select t1.a,t2.b,t3.c from table1 t1,table2 t2,table3 t3
                where 1=1 and (t1.c like :ps) and (t1.b = :p4 or (t2.f=:p2 and t3.f=:p3) )    order by 1,2
                几点需要说明：
                1，一个语句中可以有多个占位符，不同占位符转换的语句用and连接。
                2，同一个占位符中如果有多个符合条件的语句根据isUnion的值采用or或者and连接
                3，过滤条件(3)[table4.b] = {p4} 和 table4:t1 配合得到了 t1.b = :p4 语句，
                    其中table4 和from与中的table名称没有直接关系，在写查询占位符是要把别名必须是from中有的就可了。
                4，还有一点要说明的是如果一个占位符没有选择到任何的过滤条件，则这个占位符直接消失，如上面的{table9:t1}
     *
     * 方法二：内置条件语句
     *         这个方法完全根据查询参数来生成，它的形式是[(由参数构成的逻辑表达式)(需要添加到最终查询中的参数，这个内容是可选的)|语句]
     *         它实现的逻辑是先计算 有参数构成的逻辑表达式，表达式中可以用标识符来引用paramsMap中的值，比如p2，
     *         但是如果这个参数不符合单个标识符格式，比如p1.1则需要用${p1.1}来引用
     *
     *         如果(有参数构成的逻辑表达式)的值为false或者等于0的数值这个占位符直接消失，如果值为true或者不等于0的数值，则做两件事
     *             1，将'|'后面的语句会添加的查询语句。
     *             2，如果(需要添加到最终查询中的参数，这个内容是可选的)有变量，并且可以在paramsMap中，则将这些变量添加到最终查询中的参数。
     *                 变量的格式同方法一paramName:(pretreat)paramAlias，如果需要重命名则要写:paramAlias。
     *         举个例子：
                queryStatement = "select [(${p1.1} 》 2 && p2  》  2)|t1.a,] t2.b,t3.c "
                   "from [(${p1.1} 》2  && p2 》2)| table1 t1,] table2 t2,table3 t3 "
                   "where 1=1 [(${p1.1}  》 2  && p2 》2)(p1.1:ps)| and t1.a=:ps]+
                   "[(isNotEmpty(${p1.1}) && isNotEmpty(p2) && isNotEmpty(p3))(p2,p3:px)"
                   "| and (t2.b 》 :p2 or t3.c  》:px)] order by 1,2";

                System.out.println(translateQuery(queryStatement,filters,paramsMap,true).getQuery());
                结果是：
                select  t2.b,t3.c from  table2 t2,table3 t3 where 1=1 and (t2.b 》 :p2 or t3.c  》:px) order by 1,2
                这个[]占位符比可以出现在查询语句的任何位置。逻辑表达式也非常灵活，可以构建非常复杂的情况。表达式解释器内置了很多保留函数参见compiler项目

     *        这个占位符还有一个更简洁的写法，但是能力做了一定的减弱，形式为[参数,:参数,.....|语句]，解释一下：
     *            1，|前面是参数后面是语句，当所有这些参数在paramsMap中都存在时后面的语句会加入到查询中，否则自动消失。
     *            2，参数前面如果有':'这个参数对应的查询变量将添加到最终查询中的参数。所以这里的参数有三种正确的写法和一种错误的写法
     *                paramName 表示只检查变量是否存在用于判断；paramName名称中不能有','和':'
     *                paramName:paramAlias 表示不仅用于判断，还要将值以paramAlias别名添加到查询参数表中
     *                :paramAlias 这个是上面的在paramName==paramAlias的情况下的简写
     *                paramName: 这个是不允许的，切记
     *        上面的例子可以改写同时给p1.1赋一个新值为：
            paramsMap.put("p1.1", "5");
           queryStatement = "select [(${p1.1} 》2 && p2 》2)|t1.a,] t2.b,t3.c "
                "from [(${p1.1} 》2 && p2 》2)| table1 t1,] table2 t2,table3 t3 "
                "where 1=1 [(${p1.1} 》2 && p2 》2)(p1.1:ps)| and t1.a=:ps]"
                "[p1.1,:p2,p3:px| and (t2.b 》 :p2 or t3.c  》:px)] order by 1,2";
            结果是：
            select t1.a, t2.b,t3.c from  table1 t1, table2 t2,table3 t3 where 1=1
             and t1.a=:ps and (t2.b 》 :p2 or t3.c  》:px) order by 1,2
            注意需要用到参数值进行运算的逻辑表达式无法改写。

     * 方法一可以将查询条件外置，适合在不同的查询中使用共同的过滤条件。最典型的应用场景是权限过滤。
     * 方法二在编写上更优雅更灵活。适用于配合根据前端输入的查询条件值自动配置查询语句。
     *
     * 这两种方法各有优点开发人员可以选择使用，也可以同时混合使用，合理的混合使用可以给程序带来很大的便捷
     * 但不支持嵌套使用，如果一定要嵌套使用可以调用这个方法两次。
     *
     * @param queryStatement 待处理的查询语句;
     *            这个转换函数不对查询语句做任何的合法性检查，这样做开发人员可以更灵活的使用，比如，它可以仅仅是一个{}占位符，这样就可以
     *            获得一个查询条件片段，开发人员可以手动将这个片段添加的自己的查询语句中
     * @param filters    过滤条件，可以为null
     * @param paramsMap 查询参数
     * @param isUnion    拼接方式，是在同一个占位符中有多个符合条件的过滤语句时之间的拼接方式，true用Or拼接，false用and拼接。
     * @return 转换后的查询语句和这个语句中使用的查询参数，这个查询参数是paramsMap的一个子集。
     */
    public static QueryAndNamedParams translateQuery(
        String queryStatement, Collection<String> filters,
        Object paramsMap, boolean isUnion) {

        return translateQuery(queryStatement, filters,
            isUnion, new SimpleFilterTranslater(paramsMap));

    }

    /**
     * 和public static QueryAndNamedParams translateQuery(
     * String queryStatement,Collection《String》 filters,
     * Map《String,Object》 paramsMap, boolean isUnion)
     * 一样，不同的是这个方法在没有外部过滤条件的情况下使用，就是没有上面的方法一
     *
     * @param queryStatement queryStatement
     * @param paramsMap      paramsMap
     * @return QueryAndNamedParams
     */
    public static QueryAndNamedParams translateQuery(
        String queryStatement, Object paramsMap) {

        return translateQuery(queryStatement, null,
            false, new SimpleFilterTranslater(paramsMap));
    }

    /**
     * 是这个方法只生成外部过滤条件的 过滤语句片段
     *
     * @param tableMap  管理的表名 和 别名
     * @param filters   相关的过滤条件
     * @param paramsMap 参数
     * @param isUnion   拼接方式，是在同一个占位符中有多个符合条件的过滤语句时之间的拼接方式，true用Or拼接，false用and拼接
     * @return QueryAndNamedParams
     */
    public static QueryAndNamedParams translateQuery(
        Map<String, String> tableMap, Collection<String> filters,
        Object paramsMap, boolean isUnion) {

        SimpleFilterTranslater translater = new SimpleFilterTranslater(paramsMap);
        translater.setTableAlias(tableMap);

        return translateQueryFilter(filters,
            translater, isUnion);
    }

    public interface IFilterTranslater extends VariableTranslate {
        void setTableAlias(Map<String, String> tableAlias);

        String translateColumn(String columnDesc);

        LeftRightPair<String, Object> translateParam(String paramName);
    }

    /*public static SimpleFilterTranslater createFilterTranslater(Object objOrVariableTranslate){
        return new SimpleFilterTranslater(objOrVariableTranslate);
    }*/

    public static class SimpleFilterTranslater implements IFilterTranslater {
        private Object object;
        private Map<String, String> tableAlias;

        public SimpleFilterTranslater(Object paramsMap) {
            this.tableAlias = null;
            this.object = paramsMap;
        }

        @Override
        public void setTableAlias(Map<String, String> tableAlias) {
            this.tableAlias = tableAlias;
        }

        @Override
        public String translateColumn(String columnDesc) {
            if (tableAlias == null || columnDesc == null || tableAlias.size() == 0)
                return null;
            int n = columnDesc.indexOf('.');

            String poClassName = n < 0? "*" : columnDesc.substring(0, n);
            String columnName = n < 0? columnDesc : columnDesc.substring(n + 1);
            if (tableAlias.containsKey(poClassName)) {
                String alias = tableAlias.get(poClassName);
                return StringUtils.isBlank(alias) ? columnName : alias + '.' + columnName;
            } /** 这个地方无法获取 表相关的元数据信息，如果可以校验一下字段中是否有对应的字段 就完美了；、
             所以目前只能由于仅有一个表的过滤中 */
            else if ("*".equals(poClassName) && tableAlias.size() == 1) {
                String alias = tableAlias.values().iterator().next();
                return StringUtils.isBlank(alias) ? columnName : alias + '.' + columnName;
            }
            return null;
        }

        @Override
        public LeftRightPair<String, Object> translateParam(String paramName) {
            Object obj = getVarValue(paramName);
            if (obj == null)
                return null;
            if (obj instanceof String) {
                if (StringUtils.isBlank((String) obj))
                    return null;
            }
            return new LeftRightPair<>(paramName, obj);
        }

        @Override
        public Object getVarValue(String varName) {
            if (object == null)
                return null;
            if(object instanceof VariableTranslate){
                return ((VariableTranslate)object).getVarValue(varName);
            }
            return ReflectionOpt.attainExpressionValue(object, varName);
        }
    }

}
