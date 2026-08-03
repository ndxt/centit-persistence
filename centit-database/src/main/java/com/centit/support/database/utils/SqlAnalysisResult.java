package com.centit.support.database.utils;

import java.util.*;

/**
 * SQL 语句结构化分析结果。
 * <p>
 * 由 {@link SqlStatementAnalyzer#analyze(String)} 产出，描述一条（可能含参数驱动 SQL 增强语法的）
 * SQL 的语句类型、源表/目标表、列、条件字段、聚合、命名参数、增强语法元素，以及展开后的 SQL 和警告。
 * <p>
 * 设计原则：宁可标记不可解析（{@link #parseSuccess}=false 并写入 {@link #warnings}），
 * 也不伪造血缘信息。
 * <p>
 * 字段使用普通 POJO + 集合默认初始化，便于直接序列化为 JSON 供上层（如 DDE 的 sqlAnalyze 算子）输出。
 *
 * @author codefan@hotmail.com
 */
public class SqlAnalysisResult {

    /** 语句类型常量 */
    public static final String TYPE_SELECT = "SELECT";
    public static final String TYPE_INSERT = "INSERT";
    public static final String TYPE_UPDATE = "UPDATE";
    public static final String TYPE_DELETE = "DELETE";
    public static final String TYPE_MERGE = "MERGE";
    public static final String TYPE_WITH = "WITH";
    public static final String TYPE_UNKNOWN = "UNKNOWN";

    /** 增强语法元素类型：方法一外置过滤占位符 {@code {table:alias}} */
    public static final String ELEMENT_BRACE = "brace";
    /** 增强语法元素类型：方法二内置条件块 {@code [...] } */
    public static final String ELEMENT_SQUARE = "square";

    /** 整体是否解析成功（不可解析处不伪造结果） */
    private boolean parseSuccess = true;

    /** 语句类型：SELECT/INSERT/UPDATE/DELETE/MERGE/WITH/UNKNOWN */
    private String statementType = TYPE_UNKNOWN;

    /** 子类型/组合形式，如 INSERT_SELECT、UPDATE_FROM、MERGE；可为 null */
    private String subType;

    /** 源表（select from / 子查询 / update from / merge source）列表 */
    private List<TableRef> sourceTables = new ArrayList<>();

    /** 目标表（insert into / update / delete from / merge into）列表 */
    private List<String> targetTables = new ArrayList<>();

    /** 输出列（select 字段列表），含别名与原始表达式 */
    private List<ColumnRef> columns = new ArrayList<>();

    /** where 条件中引用的字段（去重、保留顺序） */
    private List<String> conditionColumns = new ArrayList<>();

    /** 出现的聚合函数（count/sum/avg/min/max 等，小写、去重） */
    private List<String> aggregates = new ArrayList<>();

    /** 命名参数名集合（{@code :name}，含增强语法中引用的参数） */
    private Set<String> parameters = new LinkedHashSet<>();

    /** 参数驱动 SQL 增强语法元素清单 */
    private List<ParamDrivenElement> paramDrivenElements = new ArrayList<>();

    /** 全展开（方法二片段全部并入、方法一占位符标注）后的可分析 SQL */
    private String resolvedSql;

    /** 解析过程中的警告（不可确定字段、未识别结构等），不含敏感值 */
    private List<String> warnings = new ArrayList<>();

    public boolean isParseSuccess() {
        return parseSuccess;
    }

    public void setParseSuccess(boolean parseSuccess) {
        this.parseSuccess = parseSuccess;
    }

    public String getStatementType() {
        return statementType;
    }

    public void setStatementType(String statementType) {
        this.statementType = statementType;
    }

    public String getSubType() {
        return subType;
    }

    public void setSubType(String subType) {
        this.subType = subType;
    }

    public List<TableRef> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(List<TableRef> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public List<String> getTargetTables() {
        return targetTables;
    }

    public void setTargetTables(List<String> targetTables) {
        this.targetTables = targetTables;
    }

    public List<ColumnRef> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnRef> columns) {
        this.columns = columns;
    }

    public List<String> getConditionColumns() {
        return conditionColumns;
    }

    public void setConditionColumns(List<String> conditionColumns) {
        this.conditionColumns = conditionColumns;
    }

    public List<String> getAggregates() {
        return aggregates;
    }

    public void setAggregates(List<String> aggregates) {
        this.aggregates = aggregates;
    }

    public Set<String> getParameters() {
        return parameters;
    }

    public void setParameters(Set<String> parameters) {
        this.parameters = parameters;
    }

    public List<ParamDrivenElement> getParamDrivenElements() {
        return paramDrivenElements;
    }

    public void setParamDrivenElements(List<ParamDrivenElement> paramDrivenElements) {
        this.paramDrivenElements = paramDrivenElements;
    }

    public String getResolvedSql() {
        return resolvedSql;
    }

    public void setResolvedSql(String resolvedSql) {
        this.resolvedSql = resolvedSql;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public void setWarnings(List<String> warnings) {
        this.warnings = warnings;
    }

    public void addWarning(String warning) {
        if (warning != null && !warning.isEmpty()) {
            this.warnings.add(warning);
        }
    }

    /** 表引用：表名 + 别名 */
    public static class TableRef {
        private String name;
        private String alias;

        public TableRef() {
        }

        public TableRef(String name, String alias) {
            this.name = name;
            this.alias = alias;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }
    }

    /** 输出列：名称、别名、原始字段表达式 */
    public static class ColumnRef {
        private String name;
        private String alias;
        private String expression;

        public ColumnRef() {
        }

        public ColumnRef(String name, String alias, String expression) {
            this.name = name;
            this.alias = alias;
            this.expression = expression;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getExpression() {
            return expression;
        }

        public void setExpression(String expression) {
            this.expression = expression;
        }
    }

    /**
     * 参数驱动 SQL 增强语法元素。
     * <ul>
     *   <li>{@link #ELEMENT_BRACE}：方法一占位符 {@code {table:alias}}，tableAlias 记录表名→别名映射，
     *       required 标记是否为 {@code {required ...}}；</li>
     *   <li>{@link #ELEMENT_SQUARE}：方法二条件块 {@code [...] }，referencedParams 为引用的参数，
     *       statementFragment 为拼入主 SQL 的语句片段（全展开）。</li>
     * </ul>
     */
    public static class ParamDrivenElement {
        /** {@link #ELEMENT_BRACE} 或 {@link #ELEMENT_SQUARE} */
        private String type;
        /** 原始文本（不含外层括号），用于定位；分析器不输出敏感值 */
        private String raw;
        /** 方法一：表名→别名映射；方法二为空 */
        private Map<String, String> tableAlias = new LinkedHashMap<>();
        /** 方法一：是否 required */
        private boolean required;
        /** 方法二：引用的参数名集合 */
        private Set<String> referencedParams = new LinkedHashSet<>();
        /** 方法二：拼入主 SQL 的语句片段（全展开后） */
        private String statementFragment;

        public ParamDrivenElement() {
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getRaw() {
            return raw;
        }

        public void setRaw(String raw) {
            this.raw = raw;
        }

        public Map<String, String> getTableAlias() {
            return tableAlias;
        }

        public void setTableAlias(Map<String, String> tableAlias) {
            this.tableAlias = tableAlias;
        }

        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }

        public Set<String> getReferencedParams() {
            return referencedParams;
        }

        public void setReferencedParams(Set<String> referencedParams) {
            this.referencedParams = referencedParams;
        }

        public String getStatementFragment() {
            return statementFragment;
        }

        public void setStatementFragment(String statementFragment) {
            this.statementFragment = statementFragment;
        }
    }
}
