package com.centit.support.database.utils;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public abstract class FieldType {
    public static final String VOID = "void";
    /**
     * 这个是 Sql Server 和 MySql 特有的自增id类型， 起始值和增加幅度（这个一旦确定不好修改，默认1，1);
     * 并且这个字段必须是 主键； 目前还没有实现
     */
    public static final String IDENTITY = "identity";
    public static final String STRING = "string";
    public static final String INTEGER = "integer";
    public static final String NUMBER = "decimal";
    public static final String FLOAT = "float";
    /**
     * 要考虑 定点数，用于存储金钱
     */
    public static final String MONEY = "money";
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String BOOLEAN = "boolean";
    public static final String DATE = "date";
    public static final String DATETIME = "datetime";
    public static final String TIMESTAMP = "timestamp";
    public static final String FILE_ID = "fileId";
    public static final String ENUM_NAME = "enum";

    public static final String TEXT = "text"; // CLOB
    public static final String BYTE_ARRAY = "bytes"; // BLOB
    public static final String BYTE_STREAM = BYTE_ARRAY;
    public static final String TEXT_STREAM = TEXT;

    public static final String FILE = "file";
    // 对象以JSON 格式 保存在 数据库中
    public static final String JSON_OBJECT = "object";
    public static final String OBJECT_LIST = "list";

    /**
     * @param st java类 名称
     * @return java类 短名
     */
    public static String trimType(String st) {
        int p = st.lastIndexOf('.');
        if (p > 0)
            return st.substring(p + 1);
        return st;
    }

    /**
     * @param columnName 数据库中的名称（代码）
     * @return 大驼峰 名称
     */
    public static String mapClassName(String columnName) {
        return mapToHumpName(columnName, true, true);
    }

    /**
     * @param columnName  字段名字符串，
     * @param firstUpCase 驼峰属性名
     * @param ignoreSingleCharPrefix  单字母_ 前缀忽略
     * @return 返回驼峰字母
     */
    public static String mapToHumpName(String columnName, boolean firstUpCase, boolean ignoreSingleCharPrefix) {
        int nl = columnName.length();
        int i = 0;
        //忽略 单字母加下划线的前缀
        if(ignoreSingleCharPrefix && nl>2 && columnName.charAt(1) == '_'){
            i=2;
        }

        StringBuilder sClassName = new StringBuilder();
        boolean upCase = firstUpCase;
        while (i < nl) {
            char currChar = columnName.charAt(i);
            i++;
            if (currChar == '_') {
                upCase = true;
                continue;
            }
            if (upCase && currChar>='a' && currChar<='z') {
                sClassName.append((char) (currChar - 32));
            } else if (!upCase && currChar>='A' && currChar<='Z' ) {
                sClassName.append((char) (currChar + 32));
            } else {
                sClassName.append(currChar);
            }
            upCase = false;
        }
        return sClassName.toString();
    }

    public static String humpNameToColumn(String humpName, boolean upCase) {
        int nl = humpName.length();
        int i = 0;
        StringBuilder columnName = new StringBuilder();
        while (i < nl) {
            char currChar = humpName.charAt(i);
            i++;
            if (currChar>='A' && currChar<='Z') {
                columnName.append('_');
                if(!upCase){
                    columnName.append((char) (currChar + 32));
                }  else {
                    columnName.append(currChar);
                }
            } else {
                if (upCase && currChar>='a' && currChar<='z') {
                    columnName.append((char) (currChar - 32));
                }  else {
                    columnName.append(currChar);
                }
            }
        }
        return columnName.toString();
    }

    /**
     * @param columnName 数据库中的名称（代码）
     * @return 小驼峰 名称
     */
    public static String mapPropName(String columnName) {
        return mapToHumpName(columnName, false, false);
    }

    /**
     * 转换到Oracle的字段
     * @param ft String
     * @return String
     */
    public static String mapToOracleColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        return switch (ft) {
            case STRING -> "varchar2";
            case IDENTITY, INTEGER, LONG -> "number(12,0)";
            case FLOAT, DOUBLE, NUMBER -> "number";
            case MONEY -> "number(30,4)";
            case BOOLEAN -> "varchar2(1)";
            case DATE, DATETIME -> "Date";
            case TIMESTAMP -> "TimeStamp";
            case TEXT, JSON_OBJECT -> "clob";//长文本
            case BYTE_ARRAY, FILE -> "blob";//大字段
            case FILE_ID -> "varchar2(64)";//默认记录文件的ID号
            case ENUM_NAME -> "varchar2(64)";//
            default -> ft;
        };
    }

    public static String mapToGBaseColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        return switch (ft) {
            case STRING -> "lvarchar"; //南大通用 GBase 8s  varchar 最多支持255 个字符，保险起见用这个
            case INTEGER -> "int";
            case IDENTITY, LONG -> "bigint";
            case FLOAT, DOUBLE, NUMBER -> "decimal";
            case MONEY -> "decimal(30,4)";
            case BOOLEAN -> "varchar(1)";
            case DATE -> "Date";
            case DATETIME, TIMESTAMP -> "datatime";
            case TEXT, JSON_OBJECT -> "TEXT"; // "clob"也可以 TEXT < 2GB , CLOB < 4TB 感觉没有必要
            case BYTE_ARRAY, FILE -> "blob";//大字段
            case FILE_ID -> "varchar(64)";//默认记录文件的ID号
            case ENUM_NAME -> "varchar(64)";//
            default -> ft;
        };
    }

    /**
     * 转换到Oracle的字段
     *
     * @param ft String
     * @return String
     */
    public static String mapToSqlServerColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        return switch (ft) {
            case STRING -> "varchar";
            case INTEGER -> "int";
            case IDENTITY -> "bigint identity(1,1)";
            case LONG -> "bigint";
            case DOUBLE, FLOAT, NUMBER -> "decimal";
            case MONEY -> "decimal(30,4)";
            case BOOLEAN -> "varchar(1)";
            case DATE, DATETIME -> "datetime";
            case TIMESTAMP -> "TimeStamp";
            case TEXT, JSON_OBJECT -> "text";//长文本
            case BYTE_ARRAY, FILE -> "VarBinary(MAX)";
            case FILE_ID -> "varchar(64)";//默认记录文件的ID号
            case ENUM_NAME -> "varchar(64)";//
            default -> ft;
        };
    }

    /**
     * 转换到Oracle的字段
     *
     * @param ft String
     * @return String
     */
    public static String mapToDB2ColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        return switch (ft) {
            case STRING -> "varchar";
            case IDENTITY -> "INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1 )";
            case INTEGER, LONG -> "INTEGER";
            case DOUBLE, FLOAT, NUMBER -> "DECIMAL";
            case MONEY -> "DECIMAL(30,4)";
            case BOOLEAN -> "varchar(1)";
            case DATE, DATETIME -> "Date";
            case TIMESTAMP -> "TimeStamp";
            case TEXT, JSON_OBJECT -> "clob(52428800)";//长文本
            case BYTE_ARRAY, FILE -> "BLOB";
            case FILE_ID -> "varchar(64)";//默认记录文件的ID号
            case ENUM_NAME -> "varchar(64)";//
            default -> ft;
        };
    }

    /**
     * 转换到Oracle的字段
     *
     * @param ft String
     * @return String
     */
    public static String mapToMySqlColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        return switch (ft) {
            case STRING -> "varchar";
            case INTEGER -> "INT";
            case IDENTITY -> "bigint AUTO_INCREMENT";
            case LONG -> "BIGINT";
            case MONEY -> "DECIMAL(30,4)";
            case FLOAT -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case NUMBER -> "DECIMAL";
            case BOOLEAN -> "varchar(1)";
            case DATE -> "Date";
            case DATETIME -> "DATETIME";
            case TIMESTAMP -> "TimeStamp";
            case TEXT, JSON_OBJECT -> "LONGTEXT";//长文本
            case FILE_ID -> "varchar(64)";//默认记录文件的ID号
            case BYTE_ARRAY, FILE -> "VARBINARY";
            case ENUM_NAME -> "varchar(64)";//
            default -> ft;
        };
    }

    public static String mapToClickHouseColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        return switch (ft) {
            case STRING -> "String";
            case INTEGER -> "Int32";
            case IDENTITY, LONG -> "Int64";
            case NUMBER -> "DECIMAL";
            case MONEY -> "Decimal32(4)";
            case FLOAT -> "Float32";
            case DOUBLE -> "Float64";
            case BOOLEAN -> "FixedString(1)";
            case DATE -> "Date";
            case DATETIME -> "Datetime";
            case TIMESTAMP -> "Datetime64";
            case TEXT, JSON_OBJECT, BYTE_ARRAY, FILE -> "String";//长文本
            case FILE_ID -> "FixedString(64)";//默认记录文件的ID号
            case ENUM_NAME -> "FixedString(64)";//
            default -> ft;
        };
    }

    public static String mapToPostgreSqlColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        return switch (ft) {
            case STRING -> "varchar";
            case INTEGER -> "integer";
            case IDENTITY -> "SERIAL";
            case LONG -> "bigint";
            case MONEY -> "money";
            case FLOAT, DOUBLE, NUMBER -> "decimal";
            case BOOLEAN -> "char(1)";
            case DATE -> "Date";
            case DATETIME, TIMESTAMP -> "TimeStamp";
            case TEXT, JSON_OBJECT -> "TEXT";//长文本
            case FILE_ID -> "varchar(64)";//默认记录文件的ID号
            case BYTE_ARRAY, FILE -> "character";
            case ENUM_NAME -> "varchar(64)";//
            default -> ft;
        };
    }

    /**
     * @param dt 数据库类别
     * @param ft 字段 java 类别
     * @return String
     */
    public static String mapToDatabaseType(String ft, DBType dt) {
        if (dt == null || StringUtils.isBlank(ft))
            return ft;
        return switch (dt) {
            case SqlServer -> mapToSqlServerColumnType(ft);
            case DB2 -> mapToDB2ColumnType(ft);
            case H2, MySql -> mapToMySqlColumnType(ft);
            case ClickHouse -> mapToClickHouseColumnType(ft);
            case PostgreSql -> mapToPostgreSqlColumnType(ft);
            case GBase -> mapToGBaseColumnType(ft);
            default -> mapToOracleColumnType(ft);
        };

    }

    public static Map<String, String> getAllTypeMap() {
        Map<String, String> fts = new HashMap<>();
        fts.put(FieldType.STRING, "字符串");
        fts.put(FieldType.INTEGER, "整型");
        fts.put(FieldType.NUMBER, "定点数");
        fts.put(FieldType.FLOAT, "浮点型");
        fts.put(FieldType.MONEY, "金额");
        fts.put(FieldType.DOUBLE, "双精度浮点型");
        fts.put(FieldType.LONG, "长整型");
        fts.put(FieldType.BOOLEAN, "布尔型");
        fts.put(FieldType.DATE, "日期型");
        fts.put(FieldType.DATETIME, "日期时间型");
        fts.put(FieldType.TIMESTAMP, "时间戳");
        fts.put(FieldType.TEXT, "大文本");
        fts.put(FieldType.FILE_ID, "文件ID");
        fts.put(FieldType.BYTE_ARRAY, "大字段");
        fts.put(FieldType.FILE, "文件");
        fts.put(FieldType.JSON_OBJECT, "JSON对象");
        fts.put(FieldType.IDENTITY, "自增主键");
        //fts.put(FieldType.OBJECT_LIST, "数据列表");
        return fts;
    }

    public static Class<?> mapToJavaType(String columnType, int scale) {
        if(StringUtils.isBlank(columnType)) {
            return null;
        }
        columnType = columnType.toUpperCase();
        if ("NUMBER".equals(columnType) ||
            "NUMERIC".equals(columnType) ||
            "DECIMAL".equals(columnType)) {
            if (scale > 0) {
                return BigDecimal.class;
            } else {
                return Long.class;
            }
        } else if (columnType.contains("CHAR") ||
            "CLOB".equals(columnType) ||
            "TEXT".equals(columnType) ||
            "FIXEDSTRING".equals(columnType) ||
            "STRING".equals(columnType) ||
            "FILEID".equals(columnType)) {
            return String.class;
        } else if ("DATE".equals(columnType) ||
            "TIME".equals(columnType) ||
            "DATETIME".equals(columnType) ||
            "SQLDATE".equals(columnType)) {
            return Date.class;
        } else if ("TIMESTAMP".equals(columnType) ||
            "DATETIME64".equals(columnType) ||
            "SQLTIMESTAMP".equals(columnType)) {
            return Timestamp.class;
        } else if ("BLOB".equals(columnType) ||
            "VARBINARY".equals(columnType) ||
            "BFILE".equals(columnType) ||
            "JSONB".equals(columnType)) {
            return byte[].class;
        } else if ("MONEY".equals(columnType) ||
            "DECIMAL32".equals(columnType) ||
            "DECIMAL64".equals(columnType) ||
            "DECIMAL128".equals(columnType)) {
            return BigDecimal.class;//FieldType.MONEY;
        } else if ("FLOAT32".equals(columnType) ||
            "FLOAT".equals(columnType)) {
            return Float.class;
        }  else if ("FLOAT64".equals(columnType) ||
            "REAL".equals(columnType) ||
            "DOUBLE PRECISION".equals(columnType) ||
            "DOUBLE".equals(columnType)) {
            return Double.class;
        } else if ("BIGINT".equals(columnType) ||
            "INT64".equals(columnType) ||
            "UINT64".equals(columnType) ||
            "LONG".equals(columnType)) {
            return Long.class;
        } else if (columnType.contains("INT")) {
            return Integer.class;
        } else if (FieldType.JSON_OBJECT.equals(columnType) ||
            "JSON".equals(columnType) ) {
            return JSON.class;
        } else if ("BOOL".equals(columnType) ||
            "BOOLEAN".equals(columnType)) {
            return Boolean.class;
        } else {
            return String.class;
        }
    }

    public static Class<?> mapToJavaType(String columnType) {
        return mapToJavaType(columnType, 0);
    }

    /**
     * map java.sql.Type to javaType
     *
     * @param dbType java.sql.Type
     * @return java type
     * @see Types
     */
    public static Class<?> mapToJavaType(int dbType) {
        return switch (dbType) {
            case -6, -5, 5, 4, 2 -> Integer.class;
            case 6, 7 -> Float.class;
            case 8 -> Double.class;
            case 3 -> Long.class;
            case 91, 92 -> Date.class;
            case 93, 2013, 2014 -> Timestamp.class;
            case -2, -3, -4, 2004 -> byte[].class;
            default -> String.class;
        };
    }

    public static String mapToFieldType(int dbType) {
        return switch (dbType) {
            case -6, -5, 5, 4, 2 -> FieldType.INTEGER;
            case 6, 7 -> FieldType.FLOAT;
            case 8 -> FieldType.DOUBLE;
            case 3 -> FieldType.LONG;
            case 91 -> FieldType.DATE;
            case 92 -> FieldType.DATETIME;
            case 93, 2013, 2014 -> FieldType.TIMESTAMP;
            case -2, -3, -4, 2004 -> FieldType.BYTE_ARRAY;
            case 2005 -> FieldType.TEXT;
            case 16 -> FieldType.BOOLEAN;
            default -> FieldType.STRING;
        };
    }

    public static String mapToFieldType(String columnType, int length, int scale) {
        if(StringUtils.isBlank(columnType)) {
            return null;
        }
        columnType = columnType.toUpperCase();
        if ("NUMBER".equals(columnType) ||
            "NUMERIC".equals(columnType) ||
            "DECIMAL".equals(columnType)) {
            return (length>0 || scale>0) ? FieldType.NUMBER : FieldType.LONG;
        } else if (columnType.contains("CHAR")){ // CHAR VARCHAR VARCHAR2 NVARCHAR NVARCHAR2 CHARACTER LVARCHAR CHARACTER VARYING
            return FieldType.STRING;
        } else if ("DATE".equals(columnType) ||
            "SQLDATE".equals(columnType) ){
            return FieldType.DATE;
        } else if("TIME".equals(columnType) ||
            "DATETIME".equals(columnType)) {
            return FieldType.DATETIME;
        } else if ("TIMESTAMP".equals(columnType) ||
            "SQLTIMESTAMP".equals(columnType)) {
            return FieldType.TIMESTAMP;
        } else if ("CLOB".equals(columnType) ||
            "NCLOB".equals(columnType) ||
            "TEXT".equals(columnType)) {
            return FieldType.TEXT;
        } else if ("BLOB".equals(columnType) ||
            "BFILE".equals(columnType) ||
            "VARBINARY".equals(columnType)) {
            return FieldType.BYTE_ARRAY;
        } else if ("FLOAT".equals(columnType)) {
            return FieldType.FLOAT;
        } else if ("DOUBLE".equals(columnType)  ||
            "REAL".equals(columnType) ||
            "DOUBLE PRECISION".equals(columnType)) {
            return FieldType.DOUBLE;
        } else  if ("BIGINT".equals(columnType) ||
            "INT64".equals(columnType) ||
            "UINT64".equals(columnType) ||
            "SERIAL".equals(columnType)) {
            return FieldType.LONG;
        } else if ("BIT".equals(columnType) || // SQL Server 有这个类型
            columnType.contains("INT") ){ ///INT  INT4 INT8 INT16 UINT32 TINYINT、SMALLINT INTEGER ) {
            return FieldType.INTEGER;
        } else if ("BOOL".equals(columnType) ||
            "BOOLEAN".equals(columnType)) {
            return FieldType.BOOLEAN;
        }
        return columnType;
    }

    public static String mapToFieldType(Class<?> javaType) {
        // 这个要重写
        if (javaType.equals(BigDecimal.class)) {
            return FieldType.MONEY;
        }
        if (javaType.equals(Integer.class) ||
            int.class == javaType) {
            return FieldType.INTEGER;
        }
        if (javaType.equals(Float.class) ||
            float.class == javaType) {
            return FieldType.FLOAT;
        }
        if (javaType.equals(Double.class) ||
            double.class == javaType) {
            return FieldType.DOUBLE;
        }
        if (javaType.equals(Long.class) ||
            long.class == javaType) {
            return FieldType.LONG;
        }
        if (String.class.isAssignableFrom(javaType)) {
            return FieldType.STRING;
        }
        if (Boolean.class.isAssignableFrom(javaType) ||
            boolean.class == javaType) {
            return FieldType.BOOLEAN;
        }
        if (javaType.isEnum()) {
            return FieldType.ENUM_NAME;
        }
        if (Timestamp.class.isAssignableFrom(javaType)) {
            return FieldType.TIMESTAMP;
        }
        if (java.util.Date.class.isAssignableFrom(javaType)) {
            return FieldType.DATETIME;
        }
        if (byte[].class == javaType) {
            return FieldType.BYTE_ARRAY;
        }
        return FieldType.JSON_OBJECT;
    }

    public static String mapToSqliteColumnType(String javaType) {
        return switch (javaType) {
            case FieldType.INTEGER, FieldType.LONG -> "INTEGER";
            case FieldType.MONEY -> "NUMERIC(20,4)"; //sqlite 不支持 DECIMAL， 后面的 (20,4) 也没有实际意义
            case FieldType.DOUBLE, FieldType.FLOAT -> "REAL";
            case FieldType.BYTE_ARRAY -> "BLOB";
            //    return "DATETIME"; // 全部设置为 TEXT
            default -> "TEXT";
        };

    }
}
