package com.centit.support.database.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlStatementAnalyzerOracleLockTest {
    private static final String SQL = "select id from lock_test where id=:id for update";

    @Test
    void detectsRealOracleLockKeywordsAndPreservesModifiers() {
        for (String suffix : new String[]{"for update", "FOR UPDATE OF t.id NOWAIT", "for\nupdate wait 5",
            "for /* comment ' */ update skip locked", "for-- comment\rupdate", "for-- comment\nupdate"}) {
            Assertions.assertTrue(SqlStatementAnalyzer.isOracleLockQuery("select * from t " + suffix, DBType.Oracle), suffix);
        }
        for (String literal : new String[]{"'for update'", "'it''s for update'", "\"for update\"",
            "q'[text ' for update]'", "q'{text ' for update}'", "q'(text ' for update)'", "q'<text ' for update>'",
            "q'!text ' for update!'", "nq'[text ' for update]'", "NQ'!text ' for update!'"}) {
            Assertions.assertFalse(SqlStatementAnalyzer.isOracleLockQuery("select " + literal + " from t", DBType.Oracle), literal);
            Assertions.assertTrue(SqlStatementAnalyzer.isOracleLockQuery("select " + literal + " from t for update", DBType.Oracle), literal);
        }
        for (String sql : new String[]{"select 1 from t -- for update", "select /* for update */ 1 from t",
            "select '/*' as x from t /* for update */", "select 'for' as x, 'update' as y from t",
            "select before_update from t", "select for_update from t", "select * from t for_update"}) {
            Assertions.assertFalse(SqlStatementAnalyzer.isOracleLockQuery(sql, DBType.Oracle), sql);
        }
        Assertions.assertFalse(SqlStatementAnalyzer.isOracleLockQuery(SQL, DBType.MySql));
        Assertions.assertFalse(SqlStatementAnalyzer.isOracleLockQuery(SQL, DBType.PostgreSql));
        Assertions.assertFalse(SqlStatementAnalyzer.isOracleLockQuery(null, DBType.Oracle));
    }
}
