package com.centit.support.test;

import com.centit.support.database.utils.SqlAnalysisResult;

/**
 * SqlStatementAnalyzer 的临时 ad-hoc 验证入口（非 JUnit）。
 * 用于批次1 重构后对参数驱动 SQL 增强语法识别/全展开做一次 sanity check。
 */
public class TestSqlStatementAnalyzer {

    public static void main(String[] args) {
        String[] cases = {
            "select a, b, count(*) as c from t1 where a = :p1 and b > :p2 group by a order by a",
            "select t1.a, t2.b from table1 t1 left join table2 t2 on t1.id = t2.id "
                + "where 1=1 {table1:t1,table2:t2} order by t1.a",
            "select [(${p1} > 2 && p2 > 2)|c1,] c2 from t2 where 1=1 [p1,:p2| and c2 > :p2]",
            "insert into t_target (a, b) select x, y from t_source where x = :k",
            "update t set a = :v where id = :id",
            "with cte1 as (select a from t1) select cte1.a, t2.b from cte1 join t2 on cte1.a = t2.a where t2.b = :w"
        };
        for (int i = 0; i < cases.length; i++) {
            String sql = cases[i];
            System.out.println("==================== case " + i + " ====================");
            System.out.flush();
            System.out.println(sql);
            System.out.flush();
            SqlAnalysisResult r;
            try {
                r = com.centit.support.database.utils.SqlStatementAnalyzer.analyze(sql);
            } catch (Throwable t) {
                System.out.println("  EXCEPTION: " + t);
                t.printStackTrace();
                continue;
            }
            System.out.println("  parseSuccess  = " + r.isParseSuccess());
            System.out.println("  statementType = " + r.getStatementType() + ", subType = " + r.getSubType());
            StringBuilder src = new StringBuilder();
            for (SqlAnalysisResult.TableRef t : r.getSourceTables()) {
                src.append(t.getName()).append("(").append(t.getAlias()).append(") ");
            }
            System.out.println("  sourceTables  = " + src);
            System.out.println("  targetTables  = " + r.getTargetTables());
            StringBuilder cols = new StringBuilder();
            for (SqlAnalysisResult.ColumnRef c : r.getColumns()) {
                cols.append(c.getName()).append("=[").append(c.getExpression()).append("] ");
            }
            System.out.println("  columns       = " + cols);
            System.out.println("  conditionCols = " + r.getConditionColumns());
            System.out.println("  aggregates    = " + r.getAggregates());
            System.out.println("  parameters    = " + r.getParameters());
            System.out.println("  paramDriven   = " + r.getParamDrivenElements().size()
                + " (brace/square elements)");
            for (SqlAnalysisResult.ParamDrivenElement e : r.getParamDrivenElements()) {
                System.out.println("    - type=" + e.getType() + " raw=[" + e.getRaw()
                    + "] tableAlias=" + e.getTableAlias() + " required=" + e.isRequired()
                    + " refs=" + e.getReferencedParams() + " stmt=[" + e.getStatementFragment() + "]");
            }
            System.out.println("  resolvedSql   = " + r.getResolvedSql());
            System.out.println("  warnings      = " + r.getWarnings());
        }
    }
}
