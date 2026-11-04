package com.centit.support.database.ddl;

import com.centit.support.database.metadata.TableField;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.FieldType;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 方言感知的物理列定义等价判定：比较两个物理列（设计草稿投影与 JDBC 回读）
 * 的类型、长度与标度是否等价。DDL 差异生成与发布状态对比共用同一判据，
 * 避免两个入口判定不一致导致结构比对状态永远无法收敛。
 */
public final class TableFieldEquivalence {
    private TableFieldEquivalence() { }

    private static final Set<String> BINARY_TYPES = Set.of("BINARY", "VARBINARY", "BINARY VARYING");

    private static String key(String name) { return name == null ? "" : name.trim().toUpperCase(Locale.ROOT); }

    /** identity 列保持原样；其余按方言归一后的物理类型名比较。 */
    public static String physicalFieldType(TableField column, DBType dbType) {
        return FieldType.IDENTITY.equals(column.getFieldType()) ? FieldType.IDENTITY
                : FieldType.mapToFieldType(column.getColumnType(), dbType);
    }

    /** Oracle JDBC NUMBER sentinel and PostgreSQL JDBC unconstrained NUMERIC readback. */
    public static boolean postgresUnboundedNumber(TableField column, DBType dbType) {
        if (dbType != DBType.PostgreSql || !FieldType.NUMBER.equals(physicalFieldType(column, dbType))) return false;
        int length = column.getMaxLength() == null ? 0 : column.getMaxLength();
        int scale = column.getScale() == null ? 0 : column.getScale();
        return scale == -127 || scale == 0 && (length == 0 || length == 131089);
    }

    // H2 DDL emits DECIMAL(p) for nonpositive source scales. Its actual scale is zero;
    // Oracle/JDBC sentinels must not survive as target precision. Other dialects retain
    // negative scales, which can carry real rounding semantics (e.g. Oracle/PostgreSQL).
    private static boolean h2Decimal(TableField column, DBType dbType) {
        return dbType == DBType.H2 && FieldType.NUMBER.equals(physicalFieldType(column, dbType));
    }

    public static int targetLength(TableField column, DBType dbType) {
        if (postgresUnboundedNumber(column, dbType)) return 0;
        int length = column.getMaxLength() == null ? 0 : column.getMaxLength();
        return h2Decimal(column, dbType) && length <= 0 ? 24 : length;
    }

    public static int targetScale(TableField column, DBType dbType) {
        if (postgresUnboundedNumber(column, dbType)) return 0;
        int scale = column.getScale() == null ? 0 : column.getScale();
        return h2Decimal(column, dbType) && scale < 0 ? 0 : scale;
    }

    /** 有长度语义的定界二进制（VARBINARY(n)）；(MAX) 与超大长度哨兵视为无界，不比较长度。 */
    public static boolean boundedBinary(TableField field, DBType dbType) {
        String type = key(field.getColumnType());
        if (type.matches(".*\\(\\s*MAX\\s*\\).*")) return false;
        if (dbType == DBType.SqlServer && Objects.equals(field.getMaxLength(), Integer.MAX_VALUE)) return false;
        return BINARY_TYPES.contains(type.replaceFirst("\\(.*\\)", "").trim());
    }

    /** 方言按原始类型、精度和标度比较；等价判定与 DDL 生成的差异判据保持同一规则。 */
    public static boolean sameDatabaseType(TableField left, TableField right, DBType dbType) {
        String type = physicalFieldType(left, dbType);
        if (!Objects.equals(type, physicalFieldType(right, dbType))) return false;
        boolean binary = boundedBinary(left, dbType);
        if (binary != boundedBinary(right, dbType)) return false;
        return !(FieldType.precisionMatters(type) || binary) || targetLength(left, dbType) == targetLength(right, dbType)
                && targetScale(left, dbType) == targetScale(right, dbType);
    }
}
