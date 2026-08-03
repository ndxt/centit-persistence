package com.centit.support.database.utils;

import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.DatetimeOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.compiler.Lexer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * SQL 通用工具方法。
 * <p>
 * 历史上本类承载了 SQL 处理的几乎所有能力（一度超过 2000 行）。为便于维护，能力已按职责拆分为：
 * <ul>
 *   <li>{@link ParamsDrivenSQL} —— 参数驱动 SQL 翻译（{@code {table:alias}} / {@code [...]} 增强语法）
 *       与参数预处理；</li>
 *   <li>{@link SqlStatementAnalyzer} —— SQL 语句结构分析（表/列/条件/聚合提取）与增强语法全展开。</li>
 * </ul>
 * 本类仅保留与具体方言无关的通用工具：SQL 值字面量构建、like 匹配串、标识符校验/清理、
 * order by 处理、分页 limit 语句构建、预处理常量（{@code SQL_PRETREAT_*}）等。
 * <p>
 * 参数驱动 SQL 翻译与参数预处理见 {@link ParamsDrivenSQL}；SQL 语句结构分析见 {@link SqlStatementAnalyzer}。
 *
 * @author codefan@hotmail.com
 */
@SuppressWarnings("unused")
public abstract class QueryUtils {

    // ==================== 预处理常量（转发至 ParamsDrivenSQL，保持外部引用兼容） ====================

    /** 表示这个参数不需要 */
    public static final String SQL_PRETREAT_NO_PARAM = ParamsDrivenSQL.SQL_PRETREAT_NO_PARAM;
    /** 转化为模式匹配字符串，字符串中间的空格、tab都会被%替换 */
    public static final String SQL_PRETREAT_LIKE = ParamsDrivenSQL.SQL_PRETREAT_LIKE;
    /** 用于like语句，只在参数后面添加一个 % */
    public static final String SQL_PRETREAT_STARTWITH = ParamsDrivenSQL.SQL_PRETREAT_STARTWITH;
    /** 用于like语句，只在参数前面添加一个 % */
    public static final String SQL_PRETREAT_ENDWITH = ParamsDrivenSQL.SQL_PRETREAT_ENDWITH;
    /** 转化为日期类型, 没有时间， 处理效果和SQL_PRETREAT_TRUNC_DAY一致 */
    public static final String SQL_PRETREAT_DATE = ParamsDrivenSQL.SQL_PRETREAT_DATE;
    /** 转化为带时间的，日期的类型 */
    public static final String SQL_PRETREAT_DATETIME = ParamsDrivenSQL.SQL_PRETREAT_DATETIME;
    /** 转化为日期类型，并计算第二天的日期（区间查询结束时间） */
    public static final String SQL_PRETREAT_NEXT_DAY = ParamsDrivenSQL.SQL_PRETREAT_NEXT_DAY;
    /** 转化为日期类型，并计算下一月的一日（区间查询结束时间） */
    public static final String SQL_PRETREAT_NEXT_MONTH = ParamsDrivenSQL.SQL_PRETREAT_NEXT_MONTH;
    /** 转化为日期类型，并计算下一年的一月一日（区间查询结束时间） */
    public static final String SQL_PRETREAT_NEXT_YEAR = ParamsDrivenSQL.SQL_PRETREAT_NEXT_YEAR;
    /** 转化为日期类型，并计算下一周周一（区间查询结束时间） */
    public static final String SQL_PRETREAT_NEXT_WEEK = ParamsDrivenSQL.SQL_PRETREAT_NEXT_WEEK;
    /** 转化为日期类型，并截断到当天（区间查询开始时间） */
    public static final String SQL_PRETREAT_TRUNC_DAY = ParamsDrivenSQL.SQL_PRETREAT_TRUNC_DAY;
    /** 转化为日期类型，并截断到当月一日（区间查询开始时间） */
    public static final String SQL_PRETREAT_TRUNC_MONTH = ParamsDrivenSQL.SQL_PRETREAT_TRUNC_MONTH;
    /** 转化为日期类型，并截断到当年一月一日（区间查询开始时间） */
    public static final String SQL_PRETREAT_TRUNC_YEAR = ParamsDrivenSQL.SQL_PRETREAT_TRUNC_YEAR;
    /** 转化为日期类型，并截断到本周周一（区间查询开始时间） */
    public static final String SQL_PRETREAT_TRUNC_WEEK = ParamsDrivenSQL.SQL_PRETREAT_TRUNC_WEEK;
    /** 转化为 2016-6-16 这样的日期字符串 */
    public static final String SQL_PRETREAT_DATESTR = ParamsDrivenSQL.SQL_PRETREAT_DATESTR;
    /** 转化为 2016-6-16 10:25:34 这样的日期和时间字符串 */
    public static final String SQL_PRETREAT_DATETIMESTR = ParamsDrivenSQL.SQL_PRETREAT_DATETIMESTR;
    /** 过滤掉所有非数字字符 */
    public static final String SQL_PRETREAT_DIGIT = ParamsDrivenSQL.SQL_PRETREAT_DIGIT;
    /** 大写 */
    public static final String SQL_PRETREAT_UPPERCASE = ParamsDrivenSQL.SQL_PRETREAT_UPPERCASE;
    /** 小写 */
    public static final String SQL_PRETREAT_LOWERCASE = ParamsDrivenSQL.SQL_PRETREAT_LOWERCASE;
    /** 转化为符合数字的字符串 */
    public static final String SQL_PRETREAT_NUMBER = ParamsDrivenSQL.SQL_PRETREAT_NUMBER;
    /** 给字符串添加 '' 使其可以拼接到 sql 语句中，并避免 sql 注入 */
    public static final String SQL_PRETREAT_QUOTASTR = ParamsDrivenSQL.SQL_PRETREAT_QUOTASTR;
    /** 应该转化为 Integer 类型，对于数据库来说和 long 没有区别，返回 Long 类型 */
    public static final String SQL_PRETREAT_INTEGER = ParamsDrivenSQL.SQL_PRETREAT_INTEGER;
    /** 转化 Long 类型 */
    public static final String SQL_PRETREAT_LONG = ParamsDrivenSQL.SQL_PRETREAT_LONG;
    /** 转化为 Double 类型 */
    public static final String SQL_PRETREAT_FLOAT = ParamsDrivenSQL.SQL_PRETREAT_FLOAT;
    /** 将对象转换为 String，如果是数组用 ',' 连接 */
    public static final String SQL_PRETREAT_STRING = ParamsDrivenSQL.SQL_PRETREAT_STRING;
    /** 转化为驼峰结构，map_to_field */
    public static final String SQL_PRETREAT_MAPTOFIELD = ParamsDrivenSQL.SQL_PRETREAT_MAPTOFIELD;
    /** 转化为下划线形式，将属性名转换为字段名 */
    public static final String SQL_PRETREAT_MAP_NAME_COLUMN = ParamsDrivenSQL.SQL_PRETREAT_MAP_NAME_COLUMN;
    /** 将字符串用 , 分割返回 String[] */
    public static final String SQL_PRETREAT_SPLITFORIN = ParamsDrivenSQL.SQL_PRETREAT_SPLITFORIN;
    /** 修改语句中的命名参数，使其能够接受多个参数以便用于 in 语句 */
    public static final String SQL_PRETREAT_CREEPFORIN = ParamsDrivenSQL.SQL_PRETREAT_CREEPFORIN;
    /** 根据数组变量循环 */
    public static final String SQL_PRETREAT_LOOP = ParamsDrivenSQL.SQL_PRETREAT_LOOP;
    /** 根据数组变量循环并用 or 连接，自动添加 and () */
    public static final String SQL_PRETREAT_LOOP_WITH_OR = ParamsDrivenSQL.SQL_PRETREAT_LOOP_WITH_OR;
    /** 将参数值拼接到 sql 对应位置，避免 sql 注入；一般用于 order by */
    public static final String SQL_PRETREAT_INPLACE = ParamsDrivenSQL.SQL_PRETREAT_INPLACE;
    /** 过滤参数中的 html 标签 */
    public static final String SQL_PRETREAT_ESCAPE_HTML = ParamsDrivenSQL.SQL_PRETREAT_ESCAPE_HTML;

    private QueryUtils() {
        throw new IllegalAccessError("Utility class");
    }

    // ==================== SQL 值字面量构建（通用工具，保留实现） ====================

    /**
     * 把字符串 string 包装成 'string'，并将字符中的 "'" 替换为 "''"。
     *
     * @param value value
     * @return 对应的 'value'
     */
    public static String buildStringForQuery(String value) {
        if (value == null || value.isEmpty())
            return "''";
        return "'" + Strings.CS.replace(value.trim(), "'", "''") + "'";
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
     * 把 string 包装成 to_char('value','yyyy-MM-dd')。
     *
     * @param value value
     * @return 对应的 to-char('value','yyyy-MM-dd')
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
     * 把 string 包装成 to-char('value','yyyy-MM-dd hh24:mi:ss')。
     *
     * @param value value
     * @return 对应的 to-char('value','yyyy-MM-dd hh24:mi:ss')
     */
    public static String buildDateTimeStringForOracle(java.util.Date value) {
        return "TO_DATE('" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd HH:mm:ss")
            + "','yyyy-MM-dd hh24:mi:ss')";
    }

    public static String buildDateTimeStringForOracle(java.sql.Date value) {
        return "TO_DATE('" + DatetimeOpt.convertDateToString(value, "yyyy-MM-dd HH:mm:ss")
            + "','yyyy-MM-dd hh24:mi:ss')";
    }

    // ==================== like 匹配串（通用工具，保留实现） ====================

    /**
     * 将 string 中的空格换成 % 作为 like 语句的匹配串。
     * 比如 "hello world" 转变为 "%hello%world%"。
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
     * 将查询变量中用于 like 语句的变量转换为 match 字符串。
     *
     * @param queryParams 查询命名变量和值对
     * @param likeParams  用于 like 的变量名
     * @return 返回在查询变量中找到的 like 变量
     */
    public static int replaceMatchParams(Map<String, Object> queryParams, Collection<String> likeParams) {
        if (likeParams == null || likeParams.isEmpty() || queryParams == null)
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
     * 将查询变量中用于 like 语句的变量转换为 match 字符串。
     *
     * @param queryParams 查询命名变量和值对
     * @param likeParams  用于 like 的变量名
     * @return 返回在查询变量中找到的 like 变量
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

    // ==================== order by / 标识符 / 清理（通用工具，保留实现） ====================

    /**
     * 过滤 order by 语句中无效信息，在可能带入乱码和注入的情况下使用。
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
        while (aWord != null && !aWord.isEmpty()) {
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
     * 整理 sql 的标识符，避免 sql 注入，只保留数字、字母、下划线和小数点。
     *
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
     * 判断是否 sql 的标识符，避免 sql 注入，只保留数字、字母、下划线和小数点。
     *
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
     * 去掉分号；和单行注释，用于 in-place 操作指令，不符合规范直接抛出异常。
     *
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
            if (Strings.CI.equalsAny(aWord,
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

    // ==================== 分页 limit 构建（通用工具，保留实现） ====================
    // 注意：这些方法内部依赖 SqlStatementAnalyzer 的结构分析能力（通过下方转发方法调用）。

    /**
     * 生成 PostgreSQL 分页查询语句。
     *
     * @param sql         sql
     * @param offset      offset
     * @param maxsize     maxsize
     * @param asParameter asParameter
     * @return String
     */
    public static String buildPostgreSqlLimitQuerySQL(String sql, int offset, int maxsize, boolean asParameter) {
        if (asParameter)
            return "select * from ("+sql+" \r\n) a " + (offset > 0 ? " limit ? offset ?" : " limit ?");
        else
            return "select * from ("+sql+" \r\n) a " + (offset > 0 ? " limit " + maxsize + " offset " + offset :
                " limit " + maxsize);
    }

    public static String buildMySqlLimitQuerySQL(String sql, int offset, int maxsize, boolean asParameter) {
        if (asParameter)
            return "select * from ("+sql+" \r\n) a " + (offset > 0 ? " limit ?, ?" : " limit ?");
        else
            return "select * from ("+sql+" \r\n) a " + (offset > 0 ? " limit " + offset+ "," + maxsize:
                " limit " + maxsize);
    }

    /**
     * 生成 Oracle 分页查询语句，不考虑 for update 语句。
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
                pagingSelect.append(" \r\n) row_ ) where rownum_ <= ? and rownum_ > ?");
            } else {
                pagingSelect.append(" \r\n) where rownum <= ?");
            }
        } else {
            if (offset > 0) {
                pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
            } else {
                pagingSelect.append("select * from ( ");
            }
            pagingSelect.append(sql);
            if (offset > 0) {
                pagingSelect.append(" \r\n) row_ ) where rownum_ <= ")
                    .append(offset + maxsize)
                    .append(" and rownum_ > ")
                    .append(offset);
            } else {
                pagingSelect.append(" \r\n) where rownum <= ").append(maxsize);
            }
        }

        return pagingSelect.toString();
    }

    /**
     * 生成 DB2 分页查询语句。
     *
     * @param sql     sql
     * @param offset  offset
     * @param maxsize maxsize
     * @return String
     */
    public static String buildDB2LimitQuerySQL(String sql, int offset, int maxsize) {
        if (offset == 0) {
            return maxsize > 1 ? sql + "\r\n fetch first " + maxsize + " rows only" :
                sql + " fetch first 1 row only";
        }
        //nest the main query in an outer select
        return "select * from ( select inner2_.*, rownumber() over(order by order of inner2_) as rownumber_ from ( "
            + sql + "\r\n fetch first " + String.valueOf(offset + maxsize) + " rows only ) as inner2_ ) as inner1_ where rownumber_ > "
            + offset + " order by rownumber_";
    }

    /**
     * 生成 SqlServer 分页查询语句。
     *
     * @param sql     sql
     * @param offset  offset
     * @param maxsize maxsize
     * @return String
     */
    public static String buildSqlServerLimitQuerySQL(String sql, int offset, int maxsize){
        if (offset > 0) {

            List<String> sqlPieces = SqlStatementAnalyzer.splitSqlByFields(sql);
            if (sqlPieces == null || sqlPieces.size() < 3)
                return sql;
            String alias_list = StringBaseOpt.objectToString(SqlStatementAnalyzer.splitSqlFieldNames(sqlPieces.get(1)));
            String whereSql = SqlStatementAnalyzer.removeOrderBy(sqlPieces.get(2));
            String oderbySql = sqlPieces.get(2).substring(whereSql.length());
            if(StringUtils.isBlank(oderbySql)){
                oderbySql = "ORDER BY CURRENT_TIMESTAMP";
            } else {
                oderbySql = oderbySql.trim();
            }
            return "WITH query AS (SELECT inner_query.* , ROW_NUMBER() OVER ( " +
                oderbySql + " ) as __row_nr__ FROM ( " +
                sqlPieces.get(0) + sqlPieces.get(1) +
                whereSql + ") inner_query ) SELECT " +
                alias_list + " FROM query WHERE __row_nr__ >" +
                offset + " AND __row_nr__ <= " + (offset + maxsize);

        } else {
            Lexer sqlLexer = new Lexer(sql, Lexer.LANG_TYPE_SQL);
            StringBuilder sqlStr = new StringBuilder(sql.length() + 20);
            String sw = sqlLexer.getAWord();
            while(StringUtils.isNotBlank(sw)){
                if(sw.equals("(")){
                    int pos = sqlLexer.getCurrPos();
                    sqlLexer.seekToRightBracket();
                    int endPos = sqlLexer.getCurrPos();
                    sqlStr.append(" ").append(sql, pos-1, endPos).append(" ");
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
        return switch (dbType) {
            case Oracle, DM, KingBase, GBase, Oscar -> buildOracleLimitQuerySQL(sql, offset, maxsize, asParameter);
            case DB2 -> buildDB2LimitQuerySQL(sql, offset, maxsize);
            case SqlServer, Access -> buildSqlServerLimitQuerySQL(sql, offset, maxsize);
            case MySql, H2, ClickHouse -> buildMySqlLimitQuerySQL(sql, offset, maxsize, asParameter);
            case PostgreSql -> buildPostgreSqlLimitQuerySQL(sql, offset, maxsize, asParameter);
            default -> throw new ObjectException(ObjectException.ORM_METADATA_EXCEPTION,
                "不支持的数据库类型：" + dbType);
        };
    }
}
