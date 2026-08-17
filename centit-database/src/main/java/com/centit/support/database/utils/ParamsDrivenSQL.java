package com.centit.support.database.utils;

import com.centit.support.algorithm.*;
import com.centit.support.common.LeftRightPair;
import com.centit.support.compiler.EmbedFunc;
import com.centit.support.compiler.Lexer;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.compiler.VariableTranslate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.text.StringEscapeUtils;

import java.util.*;

/**
 * 参数驱动 SQL 的翻译与参数预处理。
 * <p>
 * 这一类能力从 {@link QueryUtils} 拆分而来，专门负责把"参数驱动 SQL"——即在标准 SQL 中混入
 * 两种增强语法的语句——翻译为带命名参数的可执行 SQL：
 * <ul>
 *   <li>方法一（外置过滤占位符）：{@code {table:alias,table2:alias2}} 或 {@code {required table:alias}}，
 *       配合外部 filters 集合按表别名注入权限/过滤条件；</li>
 *   <li>方法二（内置条件块）：{@code [(逻辑表达式)(参数列表)|语句]} 或简易写法 {@code [参数|语句]}，
 *       按运行时参数是否存在/表达式是否成立决定是否把语句片段拼入查询。</li>
 * </ul>
 * 两种语法可混合使用，但不支持嵌套；如需嵌套可调用两次。
 * <p>
 * 本类为纯算法工具类（无 Spring / 数据库连接依赖），可被任意项目复用。
 * 原 {@code QueryUtils} 中对应的公共静态方法保留为转发入口，以保证存量调用零改动。
 *
 * @author codefan@hotmail.com
 * @see ParamsDrivenSQL#translateQuery(String, Object)
 */
@SuppressWarnings("unused")
public abstract class ParamsDrivenSQL {

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

    private ParamsDrivenSQL() {
        throw new IllegalAccessError("Utility class");
    }

    /**
     * 将命名参数 sql（:name 形式）转换为 ? 占位 sql，并按出现顺序返回参数名列表。
     *
     * @param sql sql
     * @return LeftRightPair 转换后的 sql 和参数名列表
     */
    public static LeftRightPair<String, List<String>> transNamedParamSqlToParamSql(String sql) {
        StringBuilder sqlb = new StringBuilder();
        List<String> params = new ArrayList<>();
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        int prePos = 0;
        String aWord = lex.getAWord();
        while (aWord != null && !aWord.isEmpty()) {
            if (":".equals(aWord)) {

                int curPos = lex.getCurrPos();
                if (curPos - 1 > prePos)
                    sqlb.append(sql, prePos, curPos - 1);

                aWord = lex.getAWord();
                if (aWord == null || aWord.isEmpty())
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
        while (aWord != null && !aWord.isEmpty()) {
            if (":".equals(aWord)) {
                aWord = lex.getAWord();
                if (aWord == null || aWord.isEmpty())
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
        if (aWord == null || aWord.isEmpty())
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
     *  包括 [(${p1.1} > 2 && p2 > 2)| table1 t1,]
     *      [p1.1,:p2,p3:px| and (t2.b > :p2 or t3.c > :px)]
     *  中的原始参数 p1.1,p2,p3
     * @param sql sql
     * @return 返回sql语句中所有的 命令变量（:变量名）
     */
    public static Set<String> getSqlTemplateParameters(String sql) {

        Set<String> params = new HashSet<String>();
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);

        String aWord = lex.getAWord();
        while (aWord != null && !aWord.isEmpty()) {
            if (":".equals(aWord)) {
                aWord = lex.getAWord();
                if (aWord == null || aWord.isEmpty())
                    return params;
                params.add(aWord);

            } else if (aWord.equals("[")) {
                int beginPos = lex.getCurrPos();

                lex.seekToRightSquareBracket();
                int endPos = lex.getCurrPos();
                //分析表别名， 格式为 TableNameOrClass:alias,TableNameOrClass:alias,.....
                String queryPiece = sql.substring(beginPos, endPos - 1).trim();
                Set<String> subParams = fetchParamsFromTemplateConditions(queryPiece);
                if (subParams != null && !subParams.isEmpty())
                    params.addAll(subParams);
            }
            aWord = lex.getAWord();
        }
        return params;
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
        return switch (pretreatment.toUpperCase()) {
            case SQL_PRETREAT_LIKE -> QueryUtils.getMatchString(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_STARTWITH -> StringBaseOpt.objectToString(paramValue) + "%";
            case SQL_PRETREAT_ENDWITH -> "%" + StringBaseOpt.objectToString(paramValue);
            case SQL_PRETREAT_NEXT_DAY -> DatetimeOpt.addDays(DatetimeOpt.truncateToDay(
                DatetimeOpt.castObjectToDate(paramValue)), 1);
            case SQL_PRETREAT_NEXT_MONTH -> DatetimeOpt.addMonths(DatetimeOpt.truncateToMonth(
                DatetimeOpt.castObjectToDate(paramValue)), 1);
            case SQL_PRETREAT_NEXT_YEAR -> DatetimeOpt.addYears(DatetimeOpt.truncateToYear(
                DatetimeOpt.castObjectToDate(paramValue)), 1);
            case SQL_PRETREAT_NEXT_WEEK -> DatetimeOpt.addDays(DatetimeOpt.truncateToWeek(
                DatetimeOpt.castObjectToDate(paramValue)), 7);
            case SQL_PRETREAT_TRUNC_MONTH -> DatetimeOpt.truncateToMonth(
                DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_TRUNC_YEAR -> DatetimeOpt.truncateToYear(
                DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_TRUNC_WEEK -> DatetimeOpt.truncateToWeek(
                DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_TRUNC_DAY, SQL_PRETREAT_DATE -> DatetimeOpt.truncateToDay(
                DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_DATETIME -> DatetimeOpt.castObjectToDate(paramValue);
            case SQL_PRETREAT_DATESTR -> DatetimeOpt.convertDateToString(
                DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_DATETIMESTR -> DatetimeOpt.convertDatetimeToString(
                DatetimeOpt.castObjectToDate(paramValue));
            case SQL_PRETREAT_DIGIT -> StringRegularOpt.trimDigits(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_UPPERCASE -> StringUtils.upperCase(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_LOWERCASE -> StringUtils.lowerCase(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_NUMBER -> StringRegularOpt.trimNumber(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_INTEGER, SQL_PRETREAT_LONG -> NumberBaseOpt.castObjectToLong(paramValue);
            case SQL_PRETREAT_FLOAT -> NumberBaseOpt.castObjectToDouble(paramValue);
            case SQL_PRETREAT_ESCAPE_HTML -> StringEscapeUtils.escapeHtml4(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_QUOTASTR -> QueryUtils.buildStringForQuery(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_MAPTOFIELD -> FieldType.mapPropName(StringBaseOpt.objectToString(paramValue));
            case SQL_PRETREAT_MAP_NAME_COLUMN ->
                FieldType.humpNameToColumn(StringBaseOpt.objectToString(paramValue), true);
            case SQL_PRETREAT_STRING -> StringBaseOpt.objectToString(paramValue);
            default -> paramValue;
        };
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
        if (paramValue instanceof Collection<?> valueList) {
            List<Object> retValue = new ArrayList<>();
            for (Object ov : valueList) {
                Object ro = scalarPretreatParameter(pretreatment, ov);
                if (ro != null) {
                    retValue.add(ro);
                }
            }
            if (retValue.isEmpty())
                return null;
            return retValue;
        } else if (paramValue instanceof Object[] objs) {
            List<Object> retValue = new ArrayList<>();
            for (Object ov : objs) {
                Object ro = scalarPretreatParameter(pretreatment, ov);
                if (ro != null) {
                    retValue.add(ro);
                }
            }
            if (retValue.isEmpty())
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
                value = pretreatParameter(pretreatment, value);
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
        while (aWord != null && !aWord.isEmpty()) {
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

    public static String replaceParamAsSqlString(String sql, String paramAlias, String paramSqlString) {
        Lexer varMorp = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String sWord = varMorp.getAWord();
        while (sWord != null && !sWord.isEmpty()) {
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
     * @param Translate 变量内嵌在语句中，不用参数
     * @return QueryAndNamedParams
     */
    public static QueryAndNamedParams translateQueryFilter(String filter, IFilterTranslate Translate) {
        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();
        Lexer varMorp = new Lexer(filter, Lexer.LANG_TYPE_SQL);
        StringBuilder hqlPiece = new StringBuilder();
        String sWord = varMorp.getAWord();
        int prePos = 0;
        while (sWord != null && !sWord.isEmpty()) {
            if (sWord.equals("[")) {
                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    hqlPiece.append(filter, prePos, curPos - 1);
                varMorp.seekToRightSquareBracket();//.seekTo(']');
                prePos = varMorp.getCurrPos();
                String columnDesc = filter.substring(curPos, prePos - 1).trim();

                String qp = Translate.translateColumn(columnDesc);
                if (qp == null)
                    return null;

                hqlPiece.append(qp);

            } else if (sWord.equals("{")) {
                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    hqlPiece.append(filter, prePos, curPos - 1);
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

                LeftRightPair<String, Object> paramPair = Translate.translateParam(paramName);
                if (paramPair == null)
                    return null;

                if (paramPair.getRight() != null) {
                    Object realParam = pretreatParameter(paramMeta.right, paramPair.getRight());
                    if (hasPretreatment(paramMeta.right, SQL_PRETREAT_CREEPFORIN)) {
                        QueryAndNamedParams inSt = buildInStatement(paramAlias, realParam);
                        hqlPiece.append(inSt.getQuery());
                        hqlAndParams.addAllParams(inSt.getParams());
                    } else if (hasPretreatment(paramMeta.right, SQL_PRETREAT_INPLACE)) {
                        hqlPiece.append(QueryUtils.cleanSqlStatement(StringBaseOpt.objectToString(realParam)));
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
                                                           IFilterTranslate Translate, boolean isUnion) {
        if (filters == null || filters.isEmpty())
            return null;
        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();
        StringBuilder hqlBuilder = new StringBuilder();

        int hqlPieceCount = 0;
        for (String filter : filters) {
            QueryAndNamedParams hqlPiece = translateQueryFilter(filter, Translate);
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

    /**
     * 严格翻译外置过滤条件（调用方语义，本库不做权限判断）。和历史 API 不同，本方法不会跳过无法编译的部分：
     * 任一 filter 失败、参数重名或空集合都会返回 INVALID，且不返回可执行片段。
     */
    public static StrictSqlResult translateQueryStrict(String queryStatement,
                                                       StrictSqlAccess access,
                                                       Collection<StrictSqlFilter> filters,
                                                       boolean isUnion,
                                                       IFilterTranslate translate) {
        List<StrictSqlAnchorDiagnostic> anchorDiagnostics = new ArrayList<>();
        List<StrictSqlFilterDiagnostic> filterDiagnostics = new ArrayList<>();
        if (StringUtils.isBlank(queryStatement)) {
            return StrictSqlResult.invalid(StrictSqlReasonCode.SQL_MISSING,
                anchorDiagnostics, filterDiagnostics);
        }
        if (access == null || translate == null) {
            return StrictSqlResult.invalid(StrictSqlReasonCode.FILTER_INVALID,
                anchorDiagnostics, filterDiagnostics);
        }
        List<StrictSqlFilter> strictFilters = filters == null
            ? Collections.emptyList()
            : new ArrayList<>(filters);
        if (access == StrictSqlAccess.FILTERED) {
            if (strictFilters.isEmpty()) {
                return StrictSqlResult.invalid(StrictSqlReasonCode.FILTERS_MISSING,
                    anchorDiagnostics, filterDiagnostics);
            }
            Set<String> filterIds = new HashSet<>();
            for (StrictSqlFilter filter : strictFilters) {
                if (filter == null || StringUtils.isBlank(filter.filterId())
                    || StringUtils.isBlank(filter.expression())) {
                    return StrictSqlResult.invalid(StrictSqlReasonCode.FILTER_INVALID,
                        anchorDiagnostics, filterDiagnostics);
                }
                if (!filterIds.add(filter.filterId())) {
                    return StrictSqlResult.invalid(StrictSqlReasonCode.FILTER_ID_DUPLICATED,
                        anchorDiagnostics, filterDiagnostics);
                }
            }
        }

        QueryAndNamedParams result = new QueryAndNamedParams();
        Set<String> reservedParameterNames = new HashSet<>(
            getSqlNamedParameters(queryStatement));
        StringBuilder sql = new StringBuilder();
        Lexer lexer = new Lexer(queryStatement, Lexer.LANG_TYPE_SQL);
        String word = lexer.getAWord();
        int previousPosition = 0;
        int anchorIndex = 0;
        boolean sawRequiredAnchor = false;
        while (word != null && !word.isEmpty()) {
            if ("{".equals(word)) {
                int currentPosition = lexer.getCurrPos();
                if (currentPosition - 1 > previousPosition) {
                    sql.append(queryStatement, previousPosition, currentPosition - 1);
                }
                if (!lexer.seekToRightBrace()) {
                    return StrictSqlResult.invalid(StrictSqlReasonCode.MALFORMED_TEMPLATE,
                        anchorDiagnostics, filterDiagnostics);
                }
                previousPosition = lexer.getCurrPos();
                String anchorText = queryStatement.substring(
                    currentPosition, previousPosition - 1).trim();
                StrictAnchor anchor = parseStrictAnchor(anchorText);
                if (anchor == null) {
                    return StrictSqlResult.invalid(StrictSqlReasonCode.ANCHOR_INVALID,
                        anchorDiagnostics, filterDiagnostics);
                }
                if (anchor.required()) {
                    sawRequiredAnchor = true;
                }
                if (access == StrictSqlAccess.FULL) {
                    anchorDiagnostics.add(new StrictSqlAnchorDiagnostic(anchorIndex,
                        anchor.required(), anchor.references(), 0, true,
                        StrictSqlReasonCode.READY));
                } else if (access == StrictSqlAccess.DENY) {
                    sql.append(" and 0=1 ");
                    anchorDiagnostics.add(new StrictSqlAnchorDiagnostic(anchorIndex,
                        anchor.required(), anchor.references(), 0, true,
                        StrictSqlReasonCode.READY));
                } else {
                    translate.setTableAlias(anchor.tableMap());
                    List<QueryAndNamedParams> compiled = new ArrayList<>(strictFilters.size());
                    Set<String> localParams = new HashSet<>();
                    for (int filterIndex = 0; filterIndex < strictFilters.size(); filterIndex++) {
                        StrictSqlFilter filter = strictFilters.get(filterIndex);
                        QueryAndNamedParams piece = translateQueryFilterStrict(filter.expression(),
                            translate, strictParamPrefix(anchorIndex, filterIndex));
                        if (piece == null || StringUtils.isBlank(piece.getQuery())) {
                            filterDiagnostics.add(new StrictSqlFilterDiagnostic(filter.filterId(),
                                anchorIndex, false, StrictSqlReasonCode.FILTER_NOT_APPLICABLE));
                            continue;
                        }
                        if (STRICT_FILTER_INVALID_QUERY.equals(piece.getQuery())) {
                            filterDiagnostics.add(new StrictSqlFilterDiagnostic(filter.filterId(),
                                anchorIndex, false, StrictSqlReasonCode.FILTER_INVALID));
                            return StrictSqlResult.invalid(StrictSqlReasonCode.FILTER_PARTIALLY_COMPILED,
                                anchorDiagnostics, filterDiagnostics);
                        }
                        if (containsAny(localParams, piece.getParams().keySet())
                            || containsAny(reservedParameterNames, piece.getParams().keySet())
                            || hasParameterCollision(result.getParams(), piece.getParams())) {
                            filterDiagnostics.add(new StrictSqlFilterDiagnostic(filter.filterId(),
                                anchorIndex, false, StrictSqlReasonCode.PARAMETER_COLLISION));
                            return StrictSqlResult.invalid(StrictSqlReasonCode.PARAMETER_COLLISION,
                                anchorDiagnostics, filterDiagnostics);
                        }
                        localParams.addAll(piece.getParams().keySet());
                        compiled.add(piece);
                        filterDiagnostics.add(new StrictSqlFilterDiagnostic(filter.filterId(),
                            anchorIndex, true, StrictSqlReasonCode.READY));
                    }
                    if (compiled.isEmpty()) {
                        anchorDiagnostics.add(new StrictSqlAnchorDiagnostic(anchorIndex,
                            anchor.required(), anchor.references(), 0, false,
                            StrictSqlReasonCode.REQUIRED_ANCHOR_UNCOVERED));
                        return StrictSqlResult.invalid(StrictSqlReasonCode.REQUIRED_ANCHOR_UNCOVERED,
                            anchorDiagnostics, filterDiagnostics);
                    }
                    sql.append(" and ");
                    appendStrictFilterPieces(sql, result, compiled, isUnion);
                    anchorDiagnostics.add(new StrictSqlAnchorDiagnostic(anchorIndex,
                        anchor.required(), anchor.references(), compiled.size(), true,
                        StrictSqlReasonCode.READY));
                }
                anchorIndex++;
            } else if ("[".equals(word)) {
                int currentPosition = lexer.getCurrPos();
                if (currentPosition - 1 > previousPosition) {
                    sql.append(queryStatement, previousPosition, currentPosition - 1);
                }
                if (!lexer.seekToRightSquareBracket()) {
                    return StrictSqlResult.invalid(StrictSqlReasonCode.MALFORMED_TEMPLATE,
                        anchorDiagnostics, filterDiagnostics);
                }
                previousPosition = lexer.getCurrPos();
                String queryPiece = queryStatement.substring(
                    currentPosition, previousPosition - 1).trim();
                QueryAndNamedParams piece = translateQueryPiece(queryPiece, translate);
                if (piece != null && StringUtils.isNotBlank(piece.getQuery())) {
                    if (hasParameterCollision(result.getParams(), piece.getParams())) {
                        return StrictSqlResult.invalid(StrictSqlReasonCode.PARAMETER_COLLISION,
                            anchorDiagnostics, filterDiagnostics);
                    }
                    sql.append(piece.getQuery());
                    result.addAllParams(piece.getParams());
                    reservedParameterNames.addAll(piece.getParams().keySet());
                }
            }
            word = lexer.getAWord();
        }
        sql.append(queryStatement.substring(previousPosition));
        if (access != StrictSqlAccess.FULL && !sawRequiredAnchor) {
            return StrictSqlResult.invalid(StrictSqlReasonCode.REQUIRED_ANCHOR_MISSING,
                anchorDiagnostics, filterDiagnostics);
        }
        result.setQuery(sql.toString());
        return StrictSqlResult.ready(result, anchorDiagnostics, filterDiagnostics);
    }

    private static String strictParamPrefix(int anchorIndex, int filterIndex) {
        return "__rs_a" + anchorIndex + "_f" + filterIndex + "_";
    }

    private static final String STRICT_FILTER_INVALID_QUERY = "\u0000STRICT_FILTER_INVALID\u0000";

    private static QueryAndNamedParams translateQueryFilterStrict(String filter,
                                                                  IFilterTranslate translate,
                                                                  String parameterPrefix) {
        QueryAndNamedParams result = new QueryAndNamedParams();
        Lexer lexer = new Lexer(filter, Lexer.LANG_TYPE_SQL);
        StringBuilder sql = new StringBuilder();
        String word = lexer.getAWord();
        int previousPosition = 0;
        boolean hasApplicableColumn = false;
        boolean hasNotApplicableColumn = false;
        while (word != null && !word.isEmpty()) {
            if ("[".equals(word)) {
                int currentPosition = lexer.getCurrPos();
                if (currentPosition - 1 > previousPosition) {
                    sql.append(filter, previousPosition, currentPosition - 1);
                }
                if (!lexer.seekToRightSquareBracket()) {
                    return invalidStrictFilterPiece();
                }
                previousPosition = lexer.getCurrPos();
                String columnDesc = filter.substring(currentPosition, previousPosition - 1).trim();
                if (StringUtils.isBlank(columnDesc)) {
                    return invalidStrictFilterPiece();
                }
                String column = translate.translateColumn(columnDesc);
                if (column == null) {
                    hasNotApplicableColumn = true;
                } else {
                    hasApplicableColumn = true;
                    sql.append(column);
                }
            } else if ("{".equals(word)) {
                int currentPosition = lexer.getCurrPos();
                if (currentPosition - 1 > previousPosition) {
                    sql.append(filter, previousPosition, currentPosition - 1);
                }
                if (!lexer.seekToRightBrace()) {
                    return invalidStrictFilterPiece();
                }
                previousPosition = lexer.getCurrPos();
                if (previousPosition <= currentPosition + 1) {
                    return invalidStrictFilterPiece();
                }
                String parameter = filter.substring(currentPosition, previousPosition - 1).trim();
                if (StringUtils.isBlank(parameter)) {
                    return invalidStrictFilterPiece();
                }
                ImmutableTriple<String, String, String> parameterMeta;
                try {
                    parameterMeta = parseParameter(parameter);
                } catch (RuntimeException invalidParameter) {
                    return invalidStrictFilterPiece();
                }
                String parameterName = StringUtils.isBlank(parameterMeta.left)
                    ? parameterMeta.middle : parameterMeta.left;
                String requestedAlias = StringUtils.isBlank(parameterMeta.middle)
                    ? parameterMeta.left : parameterMeta.middle;
                if (StringUtils.isBlank(parameterName) || StringUtils.isBlank(requestedAlias)) {
                    return invalidStrictFilterPiece();
                }
                LeftRightPair<String, Object> translated = translate.translateParam(parameterName);
                if (translated == null) {
                    return invalidStrictFilterPiece();
                }
                Object value = translated.getRight();
                if (isEmptyMultiValue(value)) {
                    return new QueryAndNamedParams("0=1", new LinkedHashMap<>());
                }
                if (value != null) {
                    Object realValue = pretreatParameter(parameterMeta.right, value);
                    String alias = parameterPrefix + sanitizeParameterName(requestedAlias);
                    if (hasPretreatment(parameterMeta.right, SQL_PRETREAT_CREEPFORIN)) {
                        QueryAndNamedParams inStatement = buildInStatement(alias, realValue);
                        if (StringUtils.isBlank(inStatement.getQuery())) {
                            return new QueryAndNamedParams("0=1", new LinkedHashMap<>());
                        }
                        sql.append(inStatement.getQuery());
                        if (hasParameterCollision(result.getParams(), inStatement.getParams())) {
                            return invalidStrictFilterPiece();
                        }
                        result.addAllParams(inStatement.getParams());
                    } else if (hasPretreatment(parameterMeta.right, SQL_PRETREAT_INPLACE)) {
                        sql.append(QueryUtils.cleanSqlStatement(
                            StringBaseOpt.objectToString(realValue)));
                    } else {
                        if (result.getParams().containsKey(alias)) {
                            Object previousValue = result.getParam(alias);
                            if (!Objects.equals(previousValue, realValue)) {
                                return invalidStrictFilterPiece();
                            }
                        } else {
                            result.addParam(alias, realValue);
                        }
                        sql.append(":").append(alias);
                    }
                } else if (StringUtils.isNotBlank(translated.getLeft())) {
                    sql.append(translated.getLeft());
                } else {
                    return invalidStrictFilterPiece();
                }
            }
            word = lexer.getAWord();
        }
        if (hasNotApplicableColumn && !hasApplicableColumn) {
            return null;
        }
        if (hasNotApplicableColumn) {
            return invalidStrictFilterPiece();
        }
        sql.append(filter.substring(previousPosition));
        if (StringUtils.isBlank(sql)) {
            return invalidStrictFilterPiece();
        }
        result.setQuery(sql.toString());
        return result;
    }

    private static QueryAndNamedParams invalidStrictFilterPiece() {
        return new QueryAndNamedParams(STRICT_FILTER_INVALID_QUERY, new LinkedHashMap<>());
    }

    private static boolean containsAny(Set<String> existing, Collection<String> names) {
        if (existing == null || existing.isEmpty() || names == null || names.isEmpty()) {
            return false;
        }
        for (String name : names) {
            if (existing.contains(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasParameterCollision(Map<String, Object> existing,
                                                 Map<String, Object> additional) {
        if (existing == null || existing.isEmpty() || additional == null || additional.isEmpty()) {
            return false;
        }
        for (String name : additional.keySet()) {
            if (existing.containsKey(name)) {
                return true;
            }
        }
        return false;
    }

    private static void appendStrictFilterPieces(StringBuilder sql,
                                                 QueryAndNamedParams result,
                                                 List<QueryAndNamedParams> pieces,
                                                 boolean isUnion) {
        if (pieces.size() > 1) {
            sql.append("( ");
        }
        for (int i = 0; i < pieces.size(); i++) {
            if (i > 0) {
                sql.append(isUnion ? " or " : " and ");
            }
            QueryAndNamedParams piece = pieces.get(i);
            sql.append(piece.getQuery());
            result.addAllParams(piece.getParams());
        }
        if (pieces.size() > 1) {
            sql.append(" )");
        }
    }

    private static StrictAnchor parseStrictAnchor(String anchorText) {
        if (StringUtils.isBlank(anchorText)) {
            return null;
        }
        boolean required = false;
        String text = anchorText;
        String firstWord = Lexer.getFirstWord(text);
        if ("required".equalsIgnoreCase(firstWord)) {
            required = true;
            text = text.substring(firstWord.length()).trim();
        }
        if (StringUtils.isBlank(text)) {
            return null;
        }
        List<StrictTableReference> tableReferences = new ArrayList<>();
        List<String> references = new ArrayList<>();
        Set<String> aliases = new HashSet<>();
        for (String tableDesc : text.split(",")) {
            Lexer tableLexer = new Lexer(tableDesc, Lexer.LANG_TYPE_SQL);
            String tableName = tableLexer.getAWord();
            String aliasName = tableLexer.getAWord();
            if (":".equals(aliasName)) {
                aliasName = tableLexer.getAWord();
            }
            if (StringUtils.isBlank(tableName)) {
                return null;
            }
            if (StringUtils.isBlank(aliasName)) {
                aliasName = "";
            }
            String trailing = tableLexer.getAWord();
            if (StringUtils.isNotBlank(trailing)) {
                return null;
            }
            String aliasKey = aliasName.toLowerCase(Locale.ROOT);
            if (StringUtils.isNotBlank(aliasKey) && !aliases.add(aliasKey)) {
                return null;
            }
            tableReferences.add(new StrictTableReference(tableName, aliasName));
            references.add(StringUtils.isBlank(aliasName)
                ? tableName
                : tableName + ":" + aliasName);
        }
        if (tableReferences.isEmpty()) {
            return null;
        }
        Map<String, Long> tableCounts = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (StrictTableReference reference : tableReferences) {
            tableCounts.merge(reference.tableName(), 1L, Long::sum);
        }
        Map<String, String> tableMap = new LinkedHashMap<>();
        for (StrictTableReference reference : tableReferences) {
            if (tableCounts.get(reference.tableName()) == 1L) {
                tableMap.put(reference.tableName(), reference.aliasName());
            } else if (StringUtils.isBlank(reference.aliasName())) {
                return null;
            }
            if (StringUtils.isNotBlank(reference.aliasName())) {
                tableMap.put(reference.aliasName(), reference.aliasName());
            }
        }
        return new StrictAnchor(required, tableMap, references);
    }

    private record StrictAnchor(boolean required,
                                Map<String, String> tableMap,
                                List<String> references) {
    }

    private record StrictTableReference(String tableName, String aliasName) {
    }

    private static boolean isEmptyMultiValue(Object value) {
        return value instanceof Collection<?> collection && collection.isEmpty()
            || value instanceof Object[] array && array.length == 0;
    }

    private static String sanitizeParameterName(String paramName) {
        if (StringUtils.isBlank(paramName)) {
            return "param";
        }
        String sanitized = paramName.replaceAll("[^A-Za-z0-9_]", "_");
        return StringUtils.isBlank(sanitized) ? "param" : sanitized;
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
                sql, paramAlias, QueryUtils.cleanSqlStatement(StringBaseOpt.objectToString(realParam)));
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
        String queryPiece, IFilterTranslate Translate) {

        Lexer varMorp = new Lexer(queryPiece, Lexer.LANG_TYPE_SQL);
        String aWord = varMorp.getARawWord();
        if (aWord == null || aWord.isEmpty())
            return null;

        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();

        if ("(".equals(aWord)) {
            //获取条件语句，如果条件语句没有，则返回 null
            int curPos = varMorp.getCurrPos();
            if (!varMorp.seekToRightBracket())
                return null;
            int prePos = varMorp.getCurrPos();
            String condition = queryPiece.substring(curPos, prePos - 1);

            Object sret = VariableFormula.calculate(condition, Translate);
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
                        LeftRightPair<String, Object> paramPair = Translate.translateParam(paramName);

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
                    LeftRightPair<String, Object> paramPair = Translate.translateParam(paramName);
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
        boolean isUnion, IFilterTranslate Translate) {

        QueryAndNamedParams hqlAndParams = new QueryAndNamedParams();
        Lexer varMorp = new Lexer(queryStatement, Lexer.LANG_TYPE_SQL);
        StringBuilder hqlBuilder = new StringBuilder();
        String sWord = varMorp.getAWord();
        int prePos = 0;
        while (sWord != null && !sWord.isEmpty()) {
            if (sWord.equals("{")) {

                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    hqlBuilder.append(queryStatement, prePos, curPos - 1);
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
                Translate.setTableAlias(tableMap);
                QueryAndNamedParams hqlPiece =
                    translateQueryFilter(filters,
                        Translate, isUnion);

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
                    hqlBuilder.append(queryStatement, prePos, curPos - 1);
                varMorp.seekToRightSquareBracket();
                prePos = varMorp.getCurrPos();
                //分析表别名， 格式为 TableNameOrClass:alias,TableNameOrClass:alias,.....
                String queryPiece = queryStatement.substring(curPos, prePos - 1).trim();

                QueryAndNamedParams hqlPiece =
                    translateQueryPiece(queryPiece, Translate);

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

    /**
     * 这个函数是为了满足 根据前端查询表单中的参数填写情况动态拼接查询语句条件的的需求而设计的。
     * 传统的办法是用if语句一个一个的判断，这样是可以工作的，但是这样query语句非常零碎，容易出错。
     * <p>
     * 详见 {@link ParamsDrivenSQL#translateQuery(String, Collection, Object, boolean)} 的说明，
     * 实现已迁移至本类。
     *
     * @param queryStatement 待处理的查询语句
     * @param filters        过滤条件，可以为null
     * @param paramsMap      查询参数
     * @param isUnion        同一个占位符中有多个符合条件的过滤语句时之间的拼接方式，true用Or拼接，false用and拼接
     * @return 转换后的查询语句和这个语句中使用的查询参数
     */
    public static QueryAndNamedParams translateQuery(
        String queryStatement, Collection<String> filters,
        Object paramsMap, boolean isUnion) {

        return translateQuery(queryStatement, filters,
            isUnion, new SimpleFilterTranslate(paramsMap));

    }

    /**
     * 和 {@link #translateQuery(String, Collection, Object, boolean)} 一样，
     * 不同的是这个方法在没有外部过滤条件的情况下使用，就是没有方法一。
     *
     * @param queryStatement queryStatement
     * @param paramsMap      paramsMap
     * @return QueryAndNamedParams
     */
    public static QueryAndNamedParams translateQuery(
        String queryStatement, Object paramsMap) {

        return translateQuery(queryStatement, null,
            false, new SimpleFilterTranslate(paramsMap));
    }

    /**
     * 只生成外部过滤条件的过滤语句片段。
     *
     * @param tableMap  管理的表名 和 别名
     * @param filters   相关的过滤条件
     * @param paramsMap 查询参数
     * @param isUnion   拼接方式
     * @return QueryAndNamedParams
     */
    public static QueryAndNamedParams translateQuery(
        Map<String, String> tableMap, Collection<String> filters,
        Object paramsMap, boolean isUnion) {

        SimpleFilterTranslate Translate = new SimpleFilterTranslate(paramsMap);
        Translate.setTableAlias(tableMap);

        return translateQueryFilter(filters,
            Translate, isUnion);
    }

    /**
     * 变量翻译器接口：负责把参数驱动 SQL 中的字段描述、变量名翻译为实际的列名和参数值。
     * <p>
     * 原 {@code QueryUtils.IFilterTranslate}，已迁移至本类。存量代码可通过
     * {@code QueryUtils.IFilterTranslate} 转发继续使用。
     */
    public interface IFilterTranslate extends VariableTranslate {
        void setTableAlias(Map<String, String> tableAlias);

        String translateColumn(String columnDesc);

        LeftRightPair<String, Object> translateParam(String paramName);
    }

    /**
     * 默认的 {@link IFilterTranslate} 实现：从传入的对象（Map / POJO / {@link VariableTranslate}）
     * 中按属性路径取值。原 {@code QueryUtils.SimpleFilterTranslate}，已迁移至本类。
     */
    public static class SimpleFilterTranslate implements IFilterTranslate {
        private final Object object;
        private Map<String, String> tableAlias;

        public SimpleFilterTranslate(Object paramsMap) {
            this.tableAlias = null;
            this.object = paramsMap;
        }

        @Override
        public void setTableAlias(Map<String, String> tableAlias) {
            this.tableAlias = tableAlias;
        }

        @Override
        public String translateColumn(String columnDesc) {
            if (tableAlias == null || columnDesc == null || tableAlias.isEmpty())
                return null;
            int n = columnDesc.indexOf('.');

            String poClassName = n < 0? "*" : columnDesc.substring(0, n);
            String columnName = n < 0? columnDesc : columnDesc.substring(n + 1);
            if (tableAlias.containsKey(poClassName)) {
                String alias = tableAlias.get(poClassName);
                return StringUtils.isBlank(alias) ? columnName : alias + '.' + columnName;
            } /* * 这个地方无法获取 表相关的元数据信息，如果可以校验一下字段中是否有对应的字段 就完美了；、
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
