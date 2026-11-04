package com.centit.support.database.ddl;

import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.FieldType;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DDL 文本中默认值子句的规整与补齐：发布 SQL 生成与状态对比共用的唯一文本处理逻辑。
 */
public final class TableDdlDefaults {
    private TableDdlDefaults() {
    }

    private static final Pattern DEFAULT_KEYWORD_PATTERN = Pattern.compile("\\bdefault\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern TRAILING_INLINE_COMMENT_PATTERN =
            Pattern.compile("\\s+comment\\s+'(?:''|[^'])*'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern COMMENT_BEFORE_DEFAULT_PATTERN =
            Pattern.compile("(\\s+comment\\s+'(?:''|[^'])*')\\s+(default\\s+[^,\\r\\n)]+)", Pattern.CASE_INSENSITIVE);

    /** 包装方言 DDL 生成器：create/modify/rename 语句自动补齐默认值子句并规整注释顺序。 */
    public static DDLOperations wrap(DDLOperations delegate, DBType dbType) {
        return new DefaultValueDDLOperations(delegate, dbType);
    }

    /**
     * 对比两份表定义（设计草稿与物理回读），补齐默认值或有界二进制物理定义变化的 modify 语句。
     * 主键只增不删：物理表主键可能被历史变更中列的重建（rename/drop 旧主键列）意外丢弃；
     * 已有主键与草稿不一致时不做隐式替换（删主键有风险），交由调用方的反向工程处理。
     */
    public static void appendChangeSqls(List<String> sqlList,
            TableInfo pendingTable, TableInfo metaTable, DDLOperations ddlOpt, DBType dbType) {
        if (pendingTable == null || metaTable == null || pendingTable.getColumns() == null) {
            return;
        }
        appendPrimaryKeySql(sqlList, pendingTable, metaTable);
        for (TableField pendingColumn : pendingTable.getColumns()) {
            TableField metaColumn = metaTable.findFieldByColumn(pendingColumn.getColumnName());
            if (metaColumn == null || !isDefaultValueChanged(metaColumn, pendingColumn, dbType)
                    && TableFieldEquivalence.sameDatabaseType(metaColumn, pendingColumn, dbType)
                    || hasColumnDefinitionSql(sqlList, pendingColumn.getColumnName())) {
                continue;
            }
            sqlList.addAll(ddlOpt.makeModifyColumnSqls(pendingTable.getTableName(), metaColumn, pendingColumn));
        }
    }

    private static void appendPrimaryKeySql(List<String> sqlList, TableInfo pendingTable, TableInfo metaTable) {
        List<String> pendingKeys = new ArrayList<>();
        boolean metaHasPrimaryKey = false;
        for (TableField column : pendingTable.getPkFields()) pendingKeys.add(column.getColumnName());
        if (pendingKeys.isEmpty()) return;
        if (metaTable.getColumns() != null) {
            for (TableField column : metaTable.getColumns()) {
                if (column.isPrimaryKey()) { metaHasPrimaryKey = true; break; }
            }
        }
        if (!metaHasPrimaryKey) {
            sqlList.add("alter table " + pendingTable.getTableName()
                    + " add primary key (" + String.join(", ", pendingKeys) + ")");
        }
    }

    public static String normalizeDefaultBeforeInlineComment(String sql) {
        if (StringUtils.isBlank(sql)) {
            return sql;
        }
        Matcher matcher = COMMENT_BEFORE_DEFAULT_PATTERN.matcher(sql);
        StringBuilder buffer = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(" " + matcher.group(2).trim() + matcher.group(1)));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private static boolean isDefaultValueChanged(TableField oldColumn, TableField newColumn, DBType dbType) {
        return !Objects.equals(
                normalizeDefaultValue(oldColumn.getDefaultValue(), oldColumn.getFieldType(), dbType, true),
                normalizeDefaultValue(newColumn.getDefaultValue(), newColumn.getFieldType(), dbType, false));
    }

    /** 区分未设默认值、SQL 字符串和表达式；只有 MySQL 字符列的 JDBC 裸值属于字面量。 */
    public static String normalizeDefaultValue(String value, String fieldType, DBType dbType, boolean physicalDefault) {
        String type = FieldType.normalizeFieldType(fieldType);
        boolean character = FieldType.STRING.equals(type) || FieldType.TEXT.equals(type);
        // MySQL COLUMN_DEF 字符字面量没有 SQL 引号；空字符串及其内部空白也必须保留。
        if (value != null && physicalDefault && dbType == DBType.MySql && character
                && !value.trim().startsWith("("))
            return "literal:" + value;
        String trimmed = StringUtils.trimToEmpty(value);
        if (trimmed.isEmpty()) return "";
        if (dbType == DBType.PostgreSql) {
            if (character) {
                trimmed = trimmed.replaceFirst("(?i)^(('(?:''|[^'])*'))::(?:character varying|varchar|character|bpchar|text)(?:\\(\\d+\\))?$", "$1");
            }
            if ((FieldType.DATETIME.equals(type) || FieldType.TIMESTAMP.equals(type))
                    && trimmed.matches("(?i)(?:now\\(\\)|CURRENT_TIMESTAMP(?:\\(\\))?)")) {
                return "expression:CURRENT_TIMESTAMP";
            }
        }
        // MySQL TRUE/FALSE are numeric constants, including when assigned to VARCHAR defaults.
        if (dbType == DBType.MySql && !physicalDefault
                && StringUtils.equalsAnyIgnoreCase(trimmed, "true", "false")) {
            trimmed = "true".equalsIgnoreCase(trimmed) ? "1" : "0";
        }
        String decoded = decodeUnicodeLiteral(trimmed);
        boolean quoted = trimmed.matches("'(?:''|[^'])*'") || trimmed.matches("(?i)U&'(?:''|[^'])*'");
        String literal = quoted ? unquoteLiteral(decoded) : trimmed;
        if (literal.matches("[-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?")) {
            if (character) return "literal:" + literal;
            if (type != null && java.util.Set.of(FieldType.INTEGER, FieldType.LONG, FieldType.NUMBER,
                    FieldType.DOUBLE, FieldType.FLOAT, FieldType.MONEY)
                    .contains(type.toLowerCase(Locale.ROOT)))
                return "number:" + new java.math.BigDecimal(literal).stripTrailingZeros().toPlainString();
        }
        if (quoted) return "literal:" + literal;
        // JDBC may uppercase the standalone SQL keyword; literal text and other expressions retain their spelling.
        if ("null".equalsIgnoreCase(trimmed)) return "expression:NULL";
        return "expression:" + trimmed;
    }

    /** H2 的 Unicode 字符串字面量 {@code U&'\XXXX'}（转义符默认反斜杠）；非该形态原样返回。 */
    private static String decodeUnicodeLiteral(String value) {
        if (value.length() < 4 || !value.regionMatches(true, 0, "U&'", 0, 3)
                || value.charAt(value.length() - 1) != '\'') {
            return value;
        }
        String body = value.substring(3, value.length() - 1);
        StringBuilder buffer = new StringBuilder(body.length());
        for (int index = 0; index < body.length(); index++) {
            char current = body.charAt(index);
            if (current != '\\') {
                buffer.append(current);
                continue;
            }
            if (index + 1 < body.length() && body.charAt(index + 1) == '\\') {
                buffer.append('\\');
                index++;
                continue;
            }
            if (index + 5 <= body.length()) {
                String hex = body.substring(index + 1, index + 5);
                if (hex.matches("[0-9a-fA-F]{4}")) {
                    buffer.append((char) Integer.parseInt(hex, 16));
                    index += 4;
                    continue;
                }
            }
            buffer.append(current);
        }
        return "'" + buffer + "'";
    }

    /** 单引号字面量剥壳并把内部的 {@code ''} 还原为 {@code '}；非字面量原样返回。 */
    private static String unquoteLiteral(String value) {
        if (value.length() < 2 || value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'') {
            return value;
        }
        return value.substring(1, value.length() - 1).replace("''", "'");
    }

    private static String appendDefaultValueSql(String sql, TableField column) {
        String normalizedSql = normalizeDefaultBeforeInlineComment(sql);
        if (column == null || StringUtils.isBlank(column.getDefaultValue())
                || DEFAULT_KEYWORD_PATTERN.matcher(normalizedSql).find()) {
            return normalizedSql;
        }
        String defaultClause = " default " + column.getDefaultValue().trim();
        Matcher matcher = TRAILING_INLINE_COMMENT_PATTERN.matcher(normalizedSql);
        if (matcher.find()) {
            return normalizedSql.substring(0, matcher.start()) + defaultClause + normalizedSql.substring(matcher.start());
        }
        return normalizedSql + defaultClause;
    }

    private static boolean isColumnDefinitionSql(String sql) {
        if (StringUtils.isBlank(sql)) {
            return false;
        }
        String lowerSql = sql.toLowerCase(Locale.ROOT);
        return lowerSql.contains(" add column ")
                || lowerSql.contains(" modify column ")
                || lowerSql.contains(" alter column ");
    }

    /** 仅 MySQL 方言的整列替换语句（modify column）才需要补默认值子句。 */
    private static boolean isModifyColumnSql(String sql) {
        return StringUtils.isNotBlank(sql)
                && sql.toLowerCase(Locale.ROOT).contains(" modify column ");
    }

    private static boolean hasColumnDefinitionSql(List<String> sqlList, String columnName) {
        if (sqlList == null || sqlList.isEmpty() || StringUtils.isBlank(columnName)) {
            return false;
        }
        String lookupColumnName = columnName.toLowerCase(Locale.ROOT);
        for (String sql : sqlList) {
            if (isColumnDefinitionSql(sql) && sql.toLowerCase(Locale.ROOT).contains(lookupColumnName)) {
                return true;
            }
        }
        return false;
    }

    /** 纯数据副本：复刻调用方列对象的可变拷贝语义，setColumnType 不解析类型文本。 */
    private static final class CopyOfField implements TableField {
        private String propertyName;
        private String fieldType;
        private String columnName;
        private String columnType;
        private String fieldLabelName;
        private String columnComment;
        private boolean mandatory;
        private boolean primaryKey;
        private boolean lazyFetch;
        private Integer maxLength;
        private Integer scale;
        private String defaultValue;

        private static CopyOfField of(TableField source) {
            CopyOfField copy = new CopyOfField();
            copy.propertyName = source.getPropertyName();
            copy.fieldType = source.getFieldType();
            copy.columnName = source.getColumnName();
            copy.columnType = source.getColumnType();
            copy.fieldLabelName = source.getFieldLabelName();
            copy.columnComment = source.getColumnComment();
            copy.mandatory = source.isMandatory();
            copy.primaryKey = source.isPrimaryKey();
            copy.lazyFetch = source.isLazyFetch();
            copy.maxLength = source.getMaxLength();
            copy.scale = source.getScale();
            copy.defaultValue = source.getDefaultValue();
            return copy;
        }

        @Override public String getPropertyName() { return propertyName; }
        @Override public String getFieldType() { return fieldType; }
        @Override public Class<?> getJavaType() { return FieldType.mapToJavaType(fieldType, scale == null ? 0 : scale); }
        @Override public String getColumnName() { return columnName; }
        @Override public String getColumnType() { return columnType; }
        @Override public String getFieldLabelName() { return fieldLabelName; }
        @Override public String getColumnComment() { return columnComment; }
        @Override public boolean isMandatory() { return mandatory; }
        @Override public boolean isPrimaryKey() { return primaryKey; }
        @Override public boolean isLazyFetch() { return lazyFetch; }
        @Override public Integer getMaxLength() { return maxLength; }
        @Override public Integer getScale() { return scale; }
        @Override public String getDefaultValue() { return defaultValue; }
    }

    private static class DefaultValueDDLOperations implements DDLOperations {
        private final DDLOperations delegate;
        private final DBType dbType;

        private DefaultValueDDLOperations(DDLOperations delegate, DBType dbType) {
            this.delegate = delegate;
            this.dbType = dbType;
        }

        private String preserveUnboundedNumber(String sql, TableField column) {
            if (!TableFieldEquivalence.postgresUnboundedNumber(column, dbType)) return sql;
            // Replace this column's type only; never replace defaults, literals or other columns.
            String identifier = "(?:\"?" + Pattern.quote(column.getColumnName()) + "\"?)";
            return sql.replaceFirst("(?i)(" + identifier + "\\s+(?:(?:type|set data type)\\s+)?)(?:decimal|numeric)\\s*\\([^)]*\\)", "$1numeric");
        }

        private String preserveUnboundedNumbers(String sql, TableInfo table) {
            if (table.getColumns() != null) for (TableField column : table.getColumns()) sql = preserveUnboundedNumber(sql, column);
            return sql;
        }

        @Override
        public String makeCreateSequenceSql(String s) {
            return delegate.makeCreateSequenceSql(s);
        }

        @Override
        public String makeCreateTableSql(TableInfo tableInfo) {
            return preserveUnboundedNumbers(normalizeDefaultBeforeInlineComment(delegate.makeCreateTableSql(tableInfo)), tableInfo);
        }

        @Override
        public String makeCreateTableSql(TableInfo tableInfo, boolean comment) {
            return preserveUnboundedNumbers(normalizeDefaultBeforeInlineComment(delegate.makeCreateTableSql(tableInfo, comment)), tableInfo);
        }

        @Override
        public List<String> makeTableColumnComments(TableInfo tableInfo, int i) {
            return delegate.makeTableColumnComments(tableInfo, i);
        }

        @Override
        public String makeDropTableSql(String s) {
            return delegate.makeDropTableSql(s);
        }

        @Override
        public String makeAddColumnSql(String tableName, TableField column) {
            return preserveUnboundedNumber(appendDefaultValueSql(delegate.makeAddColumnSql(tableName, column), column), column);
        }

        @Override
        public List<String> makeModifyColumnSqls(String tableName, TableField oldColumn, TableField newColumn) {
            // 方言按原始类型、精度和默认值比较；只在旧定义副本中对齐等价物理定义，不改元数据或新 SQL。
            boolean sameType = TableFieldEquivalence.sameDatabaseType(oldColumn, newColumn, dbType);
            boolean sameDefault = !isDefaultValueChanged(oldColumn, newColumn, dbType);
            if (sameType || sameDefault && !StringUtils.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue())) {
                CopyOfField normalizedOld = CopyOfField.of(oldColumn);
                normalizedOld.maxLength = sameType ? newColumn.getMaxLength() : oldColumn.getMaxLength();
                if (sameType) {
                    normalizedOld.columnType = newColumn.getColumnType();
                    normalizedOld.scale = newColumn.getScale();
                }
                if (sameDefault) normalizedOld.defaultValue = newColumn.getDefaultValue();
                oldColumn = normalizedOld;
            }
            List<String> sqlList = delegate.makeModifyColumnSqls(tableName, oldColumn, newColumn);
            if (sqlList == null || sqlList.isEmpty()) {
                return sqlList;
            }
            List<String> fixedSqlList = new ArrayList<>(sqlList.size());
            for (String sql : sqlList) {
                // H2 等方言按子句拆分（set data type / set default 等），由方言自身生成默认值子句，不能再追加
                fixedSqlList.add(preserveUnboundedNumber(isModifyColumnSql(sql) ? appendDefaultValueSql(sql, newColumn) : sql, newColumn));
            }
            return fixedSqlList;
        }

        @Override
        public String makeDropColumnSql(String tableName, String columnName) {
            return delegate.makeDropColumnSql(tableName, columnName);
        }

        @Override
        public String makeRenameColumnSql(String tableName, String oldColumnName, TableField newColumn) {
            return preserveUnboundedNumber(normalizeDefaultBeforeInlineComment(delegate.makeRenameColumnSql(tableName, oldColumnName, newColumn)), newColumn);
        }

        @Override
        public void createSequence(String s) throws SQLException {
            delegate.createSequence(s);
        }

        @Override
        public String makeCreateViewSql(String s, String s1) {
            return delegate.makeCreateViewSql(s, s1);
        }

        @Override
        public void createTable(TableInfo tableInfo) throws SQLException {
            delegate.createTable(tableInfo);
        }

        @Override
        public void dropTable(String s) throws SQLException {
            delegate.dropTable(s);
        }

        @Override
        public void addColumn(String tableName, TableField column) throws SQLException {
            delegate.addColumn(tableName, column);
        }

        @Override
        public void modifyColumn(String tableName, TableField oldColumn, TableField newColumn) throws SQLException {
            delegate.modifyColumn(tableName, oldColumn, newColumn);
        }

        @Override
        public void dropColumn(String tableName, String columnName) throws SQLException {
            delegate.dropColumn(tableName, columnName);
        }

        @Override
        public void renameColumn(String tableName, String oldColumnName, TableField newColumn) throws SQLException {
            delegate.renameColumn(tableName, oldColumnName, newColumn);
        }
    }
}
