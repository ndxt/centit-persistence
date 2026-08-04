package com.centit.support.database.utils;

import com.centit.support.algorithm.StringRegularOpt;
import com.centit.support.compiler.Lexer;
import com.centit.support.compiler.VariableFormula;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * SQL 语句结构化分析器。
 * <p>
 * 这一类能力从 {@link QueryUtils} 拆分而来，负责对（可能含参数驱动 SQL 增强语法的）SQL 文本做
 * 纯计算的结构分析，产出 {@link SqlAnalysisResult}：
 * <ul>
 *   <li>识别语句类型（select/insert/update/delete/merge/with）及组合形式（insert-select、update-from、merge）；</li>
 *   <li>提取源表/目标表、输出列、where 条件字段、聚合函数、命名参数；</li>
 *   <li>识别并"全展开"参数驱动 SQL 的两种增强语法（方法一 {@code {table:alias}} 占位符、
 *       方法二 {@code [...]} 条件块），记录为 {@link SqlAnalysisResult.ParamDrivenElement}；</li>
 *   <li>不可解析处写入警告，绝不伪造血缘。</li>
 * </ul>
 * <p>
 * 纯算法工具类：不开启数据库连接、不依赖运行时参数（默认全展开所有可能片段）。
 * 词法分析复用 {@link Lexer}（{@code LANG_TYPE_SQL}），表/列提取复用从 {@code QueryUtils}
 * 迁移来的 {@link #extraTables} / {@link #extraFieldAndTable} 等方法。
 *
 * @author codefan@hotmail.com
 * @see ParamsDrivenSQL
 */
@SuppressWarnings("unused")
public abstract class SqlStatementAnalyzer {

    /** 聚合函数名（小写） */
    private static final Set<String> AGGREGATE_FUNCS = new HashSet<>(Arrays.asList(
        "count", "sum", "avg", "min", "max", "stddev", "variance", "median"));
    /** where 子句的结束关键字 */
    private static final Set<String> WHERE_STOP_WORDS = new HashSet<>(Arrays.asList(
        "group", "order", "having", "limit", "union", "except", "intersect", "for", "window"));

    private SqlStatementAnalyzer() {
        throw new IllegalAccessError("Utility class");
    }

    // ==================== 从 QueryUtils 迁移来的 SQL 结构分析方法 ====================

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

        while (aWord != null && !aWord.isEmpty() && !"select".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
            }
            aWord = lex.getAWord();
            if (aWord == null || aWord.isEmpty())
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

        while (aWord != null && !aWord.isEmpty() && !"from".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
            }
            aWord = lex.getAWord();
            if (aWord == null || aWord.isEmpty())
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
     * 去掉 order by 语句
     *
     * @param sql sql
     * @return sql
     */
    public static boolean hasOrderBy(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String aWord = lex.getAWord();
        while (aWord != null && !aWord.isEmpty() && !"order".equalsIgnoreCase(aWord)) {
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
        while (aWord != null && !aWord.isEmpty() && !"order".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
            }
            nPos = lex.getCurrPos();
            aWord = lex.getAWord();
            if (aWord == null || aWord.isEmpty())
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

        while (aWord != null && !aWord.isEmpty() && !"group".equalsIgnoreCase(aWord)) {
            if (aWord.equals("(")) {
                lex.seekToRightBracket();
                //aWord = lex.getAWord();
            }
            aWord = lex.getAWord();
            if (aWord == null || aWord.isEmpty())
                return null;

        }
        if ("group".equalsIgnoreCase(aWord)) {
            while (aWord != null && !aWord.isEmpty() && !"by".equalsIgnoreCase(aWord)) {
                aWord = lex.getAWord();
            }
        }
        if (!"by".equalsIgnoreCase(aWord))
            return null;
        int nPos = lex.getCurrPos();
        int nEnd = nPos;

        while (aWord != null && !aWord.isEmpty() && !"order".equalsIgnoreCase(aWord)) {
            nEnd = lex.getCurrPos();
            aWord = lex.getAWord();
        }
        if (nEnd > nPos)
            return sql.substring(nPos, nEnd);
        return null;
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

        String groupByField = getGroupByField(sqlPieces.get(2));
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
        while (aWord != null && !aWord.isEmpty() && !StringUtils.equalsAnyIgnoreCase(aWord,
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
                    if (!StringUtils.equalsAnyIgnoreCase(aWord,
                        ",", "left", "right", "inner", "outer", "join")) {
                        for (String subTableName : subTtableNameMap.keySet()) {
                            tableNameMap.put(subTableName, aWord);
                        }
                        aWord = lex.getAWord();
                    } else {
                        tableNameMap.putAll(subTtableNameMap);
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
                while(aWord != null && !aWord.isEmpty()
                    && !StringUtils.equalsAnyIgnoreCase(aWord,
                    ",", "left", "right", "inner", "outer", "join",
                    "where", "group", "order", "union", "having")){
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

        while (aWord != null && !aWord.isEmpty() && !"from".equalsIgnoreCase(aWord)) {
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
        while (aWord != null && !aWord.isEmpty() && !"from".equalsIgnoreCase(aWord)) {
            int nPos2 = lex.getCurrPos();
            while (!aWord.isEmpty() && !",".equals(aWord) && !"from".equalsIgnoreCase(aWord)) {
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
        while (aWord != null && !aWord.isEmpty() && !"from".equalsIgnoreCase(aWord)) {
            while (!aWord.isEmpty() && !",".equals(aWord)
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
        while (aWord != null && !aWord.isEmpty() && !"from".equalsIgnoreCase(aWord)) {
            if (aWord.equals("[")) {
                int curPos = varMorp.getCurrPos();
                if (curPos - 1 > prePos)
                    sbSql.append(sFieldSql, prePos, curPos - 1);

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
                    sbSql.append(sFieldSql, curPos, prePos - 1);
                }
                aWord = varMorp.getAWord();
            }
            aWord = varMorp.getAWord();
        }
        sbSql.append(sFieldSql.substring(prePos));

        return splitSqlFieldNames(sbSql.toString());
    }

    // ==================== 新增：参数驱动 SQL 增强语法识别与全展开 ====================

    /**
     * 对参数驱动 SQL 做"全展开"：把方法二 {@code [...]} 条件块的语句片段全部并入主 SQL，
     * 把方法一 {@code {table:alias}} 占位符移除（无外部 filters 时不注入条件，等价于占位符消失），
     * 产出可分析的标准 SQL，并记录所有增强语法元素。
     * <p>
     * 不连库、不依赖运行时参数；可选示例参数仅用于条件求值（缺省全展开）。
     *
     * @param sql 参数驱动 SQL 文本
     * @return 分析结果（已填充 resolvedSql、paramDrivenElements、parameters；其余字段未填）
     */
    public static SqlAnalysisResult expandParamsDrivenSql(String sql) {
        SqlAnalysisResult result = new SqlAnalysisResult();
        if (StringUtils.isBlank(sql)) {
            result.setParseSuccess(false);
            result.addWarning("SQL 文本为空");
            result.setResolvedSql("");
            return result;
        }
        StringBuilder resolved = new StringBuilder(sql.length() + 32);
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        Set<String> parameters = new LinkedHashSet<>();
        List<SqlAnalysisResult.ParamDrivenElement> elements = new ArrayList<>();
        int prePos = 0;
        String sWord = lex.getAWord();
        while (sWord != null && !sWord.isEmpty()) {
            if ("{".equals(sWord)) {
                int curPos = lex.getCurrPos();
                if (curPos - 1 > prePos)
                    resolved.append(sql, prePos, curPos - 1);
                lex.seekToRightBrace();
                prePos = lex.getCurrPos();
                String desc = sql.substring(curPos, prePos - 1).trim();
                // 方法一占位符：无 filters 时直接消失（与 translateQuery 无匹配行为一致）
                SqlAnalysisResult.ParamDrivenElement el = parseBraceElement(desc);
                elements.add(el);
            } else if ("[".equals(sWord)) {
                int curPos = lex.getCurrPos();
                if (curPos - 1 > prePos)
                    resolved.append(sql, prePos, curPos - 1);
                lex.seekToRightSquareBracket();
                prePos = lex.getCurrPos();
                String piece = sql.substring(curPos, prePos - 1).trim();
                // 方法二：全展开，把语句片段并入主 SQL
                String stmt = extractStatementFragment(piece);
                SqlAnalysisResult.ParamDrivenElement el = new SqlAnalysisResult.ParamDrivenElement();
                el.setType(SqlAnalysisResult.ELEMENT_SQUARE);
                el.setRaw(piece);
                Set<String> refs = ParamsDrivenSQL.fetchParamsFromTemplateConditions(piece);
                if (refs != null) {
                    el.getReferencedParams().addAll(refs);
                    parameters.addAll(refs);
                }
                el.setStatementFragment(stmt);
                elements.add(el);
                if (StringUtils.isNotBlank(stmt)) {
                    resolved.append(stmt);
                }
            } else if (":".equals(sWord)) {
                int colonPos = lex.getCurrPos();
                String name = lex.getAWord();
                if (StringUtils.isNotBlank(name)) {
                    parameters.add(name);
                }
                // 命名参数原样保留
                if (name != null) {
                    resolved.append(sql, prePos, colonPos - 1).append(":").append(name);
                    prePos = lex.getCurrPos();
                }
            }
            sWord = lex.getAWord();
        }
        resolved.append(sql.substring(prePos));
        result.setResolvedSql(resolved.toString());
        result.setParamDrivenElements(elements);
        result.setParameters(parameters);
        return result;
    }

    /**
     * 解析方法一占位符 {@code {table:alias,...}} 或 {@code {required table:alias}}。
     */
    private static SqlAnalysisResult.ParamDrivenElement parseBraceElement(String desc) {
        SqlAnalysisResult.ParamDrivenElement el = new SqlAnalysisResult.ParamDrivenElement();
        el.setType(SqlAnalysisResult.ELEMENT_BRACE);
        el.setRaw(desc);
        String tablesDesc = desc;
        boolean required = false;
        String firstWord = Lexer.getFirstWord(desc);
        if ("required".equalsIgnoreCase(firstWord)) {
            required = true;
            tablesDesc = desc.substring(8).trim();
        }
        el.setRequired(required);
        String[] tables = tablesDesc.split(",");
        for (String tableDesc : tables) {
            Lexer tableLexer = new Lexer(tableDesc, Lexer.LANG_TYPE_SQL);
            String tableName = tableLexer.getAWord();
            String aliasName = tableLexer.getAWord();
            if (":".equals(aliasName)) {
                aliasName = tableLexer.getAWord();
            }
            if (StringUtils.isNotBlank(tableName)) {
                el.getTableAlias().put(tableName, aliasName == null ? "" : aliasName);
            }
        }
        return el;
    }

    /**
     * 从方法二条件块中提取语句片段（顶层 {@code |} 之后的内容）。
     * 兼容完整写法 {@code [(expr)(params)|stmt]} 与简易写法 {@code [params|stmt]}；
     * 若无 {@code |}，说明格式异常，返回空串（交由 warnings 处理）。
     */
    private static String extractStatementFragment(String piece) {
        Lexer lex = new Lexer(piece, Lexer.LANG_TYPE_SQL);
        int parenDepth = 0;
        int pipeStart = -1;
        String w;
        while ((w = lex.getAWord()) != null && !w.isEmpty()) {
            if ("(".equals(w)) {
                parenDepth++;
            } else if (")".equals(w)) {
                parenDepth--;
            } else if ("|".equals(w) && parenDepth == 0) {
                pipeStart = lex.getCurrPos();
                break;
            }
        }
        if (pipeStart < 0) {
            return "";
        }
        return piece.substring(pipeStart).trim();
    }

    // ==================== 新增：完整结构化分析 ====================

    /**
     * 对（可能含参数驱动 SQL 增强语法的）SQL 做完整结构化分析。
     * <p>
     * 先全展开增强语法，再识别语句类型、源表/目标表、列、条件字段、聚合与参数。
     * 不可解析处写入警告，不伪造血缘。
     *
     * @param sql SQL 文本（可含 {@code {table:alias}} / {@code [...]} 增强语法）
     * @return 结构化分析结果（不为 null）
     */
    public static SqlAnalysisResult analyze(String sql) {
        SqlAnalysisResult result = expandParamsDrivenSql(sql);
        String resolved = result.getResolvedSql();
        if (!result.isParseSuccess() || StringUtils.isBlank(resolved)) {
            return result;
        }
        // 语句类型
        String type = detectStatementType(resolved);
        result.setStatementType(type);
        try {
            switch (type) {
                case SqlAnalysisResult.TYPE_SELECT, SqlAnalysisResult.TYPE_WITH ->
                    analyzeSelect(resolved, result);
                case SqlAnalysisResult.TYPE_INSERT -> analyzeInsert(resolved, result);
                case SqlAnalysisResult.TYPE_UPDATE -> analyzeUpdate(resolved, result);
                case SqlAnalysisResult.TYPE_DELETE -> analyzeDelete(resolved, result);
                case SqlAnalysisResult.TYPE_MERGE -> analyzeMerge(resolved, result);
                default -> result.addWarning("无法识别的语句类型，仅返回增强语法展开结果");
            }
        } catch (Exception e) {
            result.setParseSuccess(false);
            result.addWarning("结构分析异常：" + e.getMessage());
        }
        return result;
    }

    /** 识别语句首个关键字（跳过前导括号）作为语句类型 */
    private static String detectStatementType(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w = lex.getAWord();
        while (w != null && !w.isEmpty() && "(".equals(w)) {
            lex.seekToRightBracket();
            w = lex.getAWord();
        }
        if (w == null || w.isEmpty()) {
            return SqlAnalysisResult.TYPE_UNKNOWN;
        }
        return switch (w.toUpperCase()) {
            case "SELECT" -> SqlAnalysisResult.TYPE_SELECT;
            case "WITH" -> SqlAnalysisResult.TYPE_WITH;
            case "INSERT" -> SqlAnalysisResult.TYPE_INSERT;
            case "UPDATE" -> SqlAnalysisResult.TYPE_UPDATE;
            case "DELETE" -> SqlAnalysisResult.TYPE_DELETE;
            case "MERGE" -> SqlAnalysisResult.TYPE_MERGE;
            default -> SqlAnalysisResult.TYPE_UNKNOWN;
        };
    }

    /** 分析 select / with（CTE）语句：输出列、源表、条件字段、聚合 */
    private static void analyzeSelect(String sql, SqlAnalysisResult result) {
        // with CTE：CTE 定义的临时表名记为源表，主查询仍按 select 分析
        if (SqlAnalysisResult.TYPE_WITH.equals(result.getStatementType())) {
            collectCteNames(sql, result);
        }
        Pair<List<Pair<String, String>>, Map<String, String>> fat = extraFieldAndTable(sql);
        if (fat != null) {
            List<Pair<String, String>> fieldMap = fat.getLeft();
            if (fieldMap != null) {
                for (Pair<String, String> p : fieldMap) {
                    String expr = p.getRight();
                    result.getColumns().add(new SqlAnalysisResult.ColumnRef(p.getLeft(), null, expr));
                    collectAggregates(expr, result.getAggregates());
                }
            }
            Map<String, String> tables = fat.getRight();
            if (tables != null) {
                for (Map.Entry<String, String> e : tables.entrySet()) {
                    result.getSourceTables().add(new SqlAnalysisResult.TableRef(e.getKey(), e.getValue()));
                }
            }
        } else {
            result.addWarning("select 字段/表结构无法完整解析（可能含 * 或非常规语法）");
            result.setParseSuccess(false);
        }
        collectConditionColumns(sql, result.getConditionColumns());
    }

    /** 收集 with 语句中定义的 CTE 名（作为虚拟源表，alias 为空），并提取 CTE 定义体内引用的真实表，保证血缘完整 */
    private static void collectCteNames(String sql, SqlAnalysisResult result) {
        // 匹配 "with name as (" 或 ", name as (" 形式的 CTE 定义
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
            "(?i)(?:\\bwith\\b|,)\\s*([a-z_][a-z0-9_$]*)\\s+as\\s*\\(").matcher(sql);
        while (m.find()) {
            String name = m.group(1);
            if (!isSqlKeyWord(name)) {
                result.getSourceTables().add(new SqlAnalysisResult.TableRef(name, ""));
            }
            // 提取 CTE 定义体括号内的真实表，避免血缘只停留在 CTE 名层级
            String body = extractBalancedParen(sql, m.end() - 1);
            if (body != null) {
                collectInnerTables(body, result);
            }
        }
    }

    /** 从 sql[start]='(' 起做括号配对，返回匹配括号内的内容（不含外层括号）；不匹配返回 null。 */
    private static String extractBalancedParen(String sql, int start) {
        if (start < 0 || start >= sql.length() || sql.charAt(start) != '(') {
            return null;
        }
        int depth = 0;
        for (int i = start; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return sql.substring(start + 1, i);
                }
            }
        }
        return null;
    }

    /** 提取一段（子查询/CTE 定义体）内的源表并入结果；解析失败静默忽略，不伪造。 */
    private static void collectInnerTables(String subSql, SqlAnalysisResult result) {
        try {
            Pair<List<Pair<String, String>>, Map<String, String>> fat = extraFieldAndTable(subSql);
            if (fat != null && fat.getRight() != null) {
                for (Map.Entry<String, String> e : fat.getRight().entrySet()) {
                    result.getSourceTables().add(new SqlAnalysisResult.TableRef(e.getKey(), e.getValue()));
                }
            }
        } catch (Exception ignore) {
            // 子结构解析失败不影响主流程，避免伪造血缘
        }
    }

    /** 分析 insert：目标表 + 可选 insert-select 源表 */
    private static void analyzeInsert(String sql, SqlAnalysisResult result) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w = lex.getAWord(); // insert
        while (w != null && !w.isEmpty() && !"into".equalsIgnoreCase(w)) {
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            }
            w = lex.getAWord();
        }
        if ("into".equalsIgnoreCase(w)) {
            String table = readNextIdentifier(lex);
            if (StringUtils.isNotBlank(table)) {
                result.getTargetTables().add(table);
            }
        }
        // insert-select：若包含 select，提取源表
        if (containsSelect(sql)) {
            result.setSubType("INSERT_SELECT");
            collectSelectSourceTables(sql, result);
        }
    }

    /** 分析 update：目标表 + 可选 update-from 源表 */
    private static void analyzeUpdate(String sql, SqlAnalysisResult result) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w = lex.getAWord(); // update
        String table = readNextIdentifier(lex);
        String targetAlias = null;
        if (StringUtils.isNotBlank(table)) {
            String alias = readNextIdentifier(lex);
            // update t1 set ... from t2 或 update t1 from t2
            if (alias != null && !"set".equalsIgnoreCase(alias)) {
                // from 是子句关键字；其余非关键字按 update t1 [as] alias set ... 的别名分析
                if ("as".equalsIgnoreCase(alias)) {
                    targetAlias = readNextIdentifier(lex);
                } else if (!"from".equalsIgnoreCase(alias) && !isSqlKeyWord(alias)) {
                    targetAlias = alias;
                }
            }
        }
        boolean updateFrom = containsWord(sql, "from");
        if (updateFrom) {
            result.setSubType("UPDATE_FROM");
            collectFromSourceTables(sql, result);
        }
        // SQL Server 支持 update alias set ... from real_table alias：按 from 中的别名还原目标表名。
        if (StringUtils.isNotBlank(table) && targetAlias == null && updateFrom) {
            for (SqlAnalysisResult.TableRef sourceTable : result.getSourceTables()) {
                if (table.equalsIgnoreCase(sourceTable.getAlias())) {
                    table = sourceTable.getName();
                    break;
                }
            }
        }
        if (StringUtils.isNotBlank(table)) {
            result.getTargetTables().add(table);
        }
        collectConditionColumns(sql, result.getConditionColumns());
    }

    /** 分析 delete：目标表（delete from t） */
    private static void analyzeDelete(String sql, SqlAnalysisResult result) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w = lex.getAWord(); // delete
        while (w != null && !w.isEmpty() && !"from".equalsIgnoreCase(w)) {
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            }
            w = lex.getAWord();
        }
        if ("from".equalsIgnoreCase(w)) {
            String table = readNextIdentifier(lex);
            if (StringUtils.isNotBlank(table)) {
                result.getTargetTables().add(table);
            }
        } else {
            // delete t from ... 形式：delete 后即目标表
            String table = readFirstIdentifierAfter(sql, "delete");
            if (StringUtils.isNotBlank(table)) {
                result.getTargetTables().add(table);
            }
        }
        collectConditionColumns(sql, result.getConditionColumns());
    }

    /** 分析 merge：目标表（into）+ 源（using） */
    private static void analyzeMerge(String sql, SqlAnalysisResult result) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w = lex.getAWord(); // merge
        while (w != null && !w.isEmpty() && !"into".equalsIgnoreCase(w)) {
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            }
            w = lex.getAWord();
        }
        if ("into".equalsIgnoreCase(w)) {
            String table = readNextIdentifier(lex);
            if (StringUtils.isNotBlank(table)) {
                result.getTargetTables().add(table);
            }
        }
        // using 后是源（表名或子查询）：子查询时提取其内部表，保证血缘完整
        Lexer lex2 = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String s;
        while ((s = lex2.getAWord()) != null && !s.isEmpty() && !"using".equalsIgnoreCase(s)) {
            if ("(".equals(s)) {
                lex2.seekToRightBracket();
            }
        }
        if ("using".equalsIgnoreCase(s)) {
            int afterUsingStart = lex2.getCurrPos();
            String afterUsing = afterUsingStart <= sql.length() ? sql.substring(afterUsingStart).trim() : "";
            if (afterUsing.startsWith("(")) {
                String body = extractBalancedParen(afterUsing, 0);
                if (body != null) {
                    collectInnerTables(body, result);
                }
            } else {
                Lexer srcLex = new Lexer(afterUsing, Lexer.LANG_TYPE_SQL);
                String src = readNextIdentifier(srcLex);
                if (StringUtils.isNotBlank(src)) {
                    result.getSourceTables().add(new SqlAnalysisResult.TableRef(src, null));
                }
            }
        }
    }

    /** 读取 Lexer 下一个标识符（跳过括号/运算符） */
    private static String readNextIdentifier(Lexer lex) {
        String w = lex.getAWord();
        while (w != null && !w.isEmpty()) {
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            } else if (Lexer.isLabel(w)) {
                return w;
            }
            w = lex.getAWord();
        }
        return null;
    }

    /** 读取某关键字之后第一个标识符 */
    private static String readFirstIdentifierAfter(String sql, String keyword) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w;
        while ((w = lex.getAWord()) != null && !w.isEmpty()) {
            if (keyword.equalsIgnoreCase(w)) {
                return readNextIdentifier(lex);
            }
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            }
        }
        return null;
    }

    /** 是否包含 select 关键字（跳过括号） */
    private static boolean containsSelect(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w;
        while ((w = lex.getAWord()) != null && !w.isEmpty()) {
            if ("select".equalsIgnoreCase(w)) {
                return true;
            }
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            }
        }
        return false;
    }

    private static boolean containsWord(String sql, String word) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w;
        while ((w = lex.getAWord()) != null && !w.isEmpty()) {
            if (word.equalsIgnoreCase(w)) {
                return true;
            }
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            }
        }
        return false;
    }

    /** 提取语句中第一个 select ... from 的源表 */
    private static void collectSelectSourceTables(String sql, SqlAnalysisResult result) {
        try {
            Map<String, String> tables = extraTables(SqlStatementAnalyzerFirstFrom(sql));
            for (Map.Entry<String, String> e : tables.entrySet()) {
                result.getSourceTables().add(new SqlAnalysisResult.TableRef(e.getKey(), e.getValue()));
            }
        } catch (Exception e) {
            result.addWarning("insert-select 源表未能完整解析");
        }
    }

    /** 取 from 之后片段供 extraTables 使用 */
    private static String collectFromSourceTables(String sql, SqlAnalysisResult result) {
        try {
            List<String> pieces = splitSqlByFields(sql);
            if (pieces != null && pieces.size() > 2) {
                Map<String, String> tables = extraTables(pieces.get(2));
                for (Map.Entry<String, String> e : tables.entrySet()) {
                    result.getSourceTables().add(new SqlAnalysisResult.TableRef(e.getKey(), e.getValue()));
                }
            }
        } catch (Exception e) {
            result.addWarning("update-from 源表未能完整解析");
        }
        return sql;
    }

    /** insert-select 中定位第一个 from 之后的片段 */
    private static String SqlStatementAnalyzerFirstFrom(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w;
        while ((w = lex.getAWord()) != null && !w.isEmpty()) {
            if ("from".equalsIgnoreCase(w)) {
                return sql.substring(lex.getCurrPos());
            }
            if ("(".equals(w)) {
                lex.seekToRightBracket();
            }
        }
        return sql;
    }

    /** 提取 where 条件中的列引用（别名.字段 形式，可靠；独立标识符作为近似补充）
        提取 where 条件中的列引用（仅 alias.column 形式，可靠；独立列名不近似伪造） */
    private static void collectConditionColumns(String sql, List<String> out) {
        String whereClause = extractWhereClause(sql);
        if (whereClause == null) {
            return;
        }
        Set<String> seen = new LinkedHashSet<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
            "\\b([a-z_][a-z0-9_$]*)\\.([a-z_][a-z0-9_$]*)\\b").matcher(whereClause);
        while (m.find()) {
            seen.add(m.group(1) + "." + m.group(2));
        }
        out.addAll(seen);
    }

    /** 截取 where 到 group/order/having/limit 之间的片段 */
    private static String extractWhereClause(String sql) {
        Lexer lex = new Lexer(sql, Lexer.LANG_TYPE_SQL);
        String w;
        while ((w = lex.getAWord()) != null && !w.isEmpty()) {
            if ("(".equals(w)) {
                lex.seekToRightBracket();
                continue;
            }
            if ("where".equalsIgnoreCase(w)) {
                int start = lex.getCurrPos();
                int end = sql.length();
                Lexer lex2 = new Lexer(sql.substring(start), Lexer.LANG_TYPE_SQL);
                String s;
                while ((s = lex2.getAWord()) != null && !s.isEmpty()) {
                    if ("(".equals(s)) {
                        lex2.seekToRightBracket();
                        continue;
                    }
                    if (WHERE_STOP_WORDS.contains(s.toLowerCase())) {
                        end = start + lex2.getCurrPos() - s.length() - 1;
                        break;
                    }
                }
                return sql.substring(start, Math.max(start, end));
            }
        }
        return null;
    }

    /** 从字段表达式中识别聚合函数名 */
    private static void collectAggregates(String expr, List<String> out) {
        if (StringUtils.isBlank(expr)) {
            return;
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
            "(?i)\\b(count|sum|avg|min|max|stddev|variance|median)\\s*\\(").matcher(expr);
        while (m.find()) {
            String fn = m.group(1).toLowerCase();
            if (!out.contains(fn)) {
                out.add(fn);
            }
        }
    }

    private static boolean isSqlKeyWord(String w) {
        if (w == null) return false;
        String u = w.toLowerCase();
        return u.equals("where") || u.equals("from") || u.equals("select") || u.equals("group")
            || u.equals("order") || u.equals("by") || u.equals("having") || u.equals("limit")
            || u.equals("and") || u.equals("or") || u.equals("not") || u.equals("in") || u.equals("like")
            || u.equals("is") || u.equals("null") || u.equals("between") || u.equals("as")
            || u.equals("join") || u.equals("on") || u.equals("inner") || u.equals("left")
            || u.equals("right") || u.equals("outer") || u.equals("union") || u.equals("case")
            || u.equals("when") || u.equals("then") || u.equals("else") || u.equals("end")
            || u.equals("distinct") || u.equals("all") || u.equals("exists") || u.equals("with");
    }
}
