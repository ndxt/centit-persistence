package com.centit.support.database.utils;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
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
     *
     * @param ft String
     * @return String
     */
    public static String mapToOracleColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        switch (ft) {
            case STRING:
                return "varchar2";
            case IDENTITY:
            case INTEGER:
            case LONG:
                return "number(12,0)";
            case FLOAT:
            case DOUBLE:
                return "number";
            case MONEY:
                return "number(30,4)";
            case BOOLEAN:
                return "varchar2(1)";
            case DATE:
            case DATETIME:
                return "Date";
            case TIMESTAMP:
                return "TimeStamp";
            case TEXT:
            case JSON_OBJECT:
                return "clob";//长文本
            case BYTE_ARRAY:
            case FILE:
                return "blob";//大字段
            case FILE_ID:
                return "varchar2(64)";//默认记录文件的ID号
            case ENUM_NAME:
                return "varchar2(64)";//
            default:
                return ft;
        }
    }

    public static String mapToGBaseColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        switch (ft) {
            case STRING:
                return "lvarchar";
            case INTEGER:
                return "int";
            case IDENTITY:
            case LONG:
                return "bigint";
            case FLOAT:
            case DOUBLE:
                return "decimal";
            case MONEY:
                return "decimal(30,4)";
            case BOOLEAN:
                return "varchar(1)";
            case DATE:
                return "Date";
            case DATETIME:
            case TIMESTAMP:
                return "datatime";
            case TEXT:
            case JSON_OBJECT:
                return "clob";//长文本
            case BYTE_ARRAY:
            case FILE:
                return "blob";//大字段
            case FILE_ID:
                return "varchar(64)";//默认记录文件的ID号
            case ENUM_NAME:
                return "varchar(64)";//
            default:
                return ft;
        }
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
        switch (ft) {
            case STRING:
                return "varchar";
            case INTEGER:
                return "int";
            case IDENTITY:
                return "bigint identity(1,1)";
            case LONG:
                return "bigint";
            case DOUBLE:
            case FLOAT:
                return "decimal";
            case MONEY:
                return "decimal(30,4)";
            case BOOLEAN:
                return "varchar(1)";
            case DATE:
            case DATETIME:
                return "datetime";
            case TIMESTAMP:
                return "TimeStamp";
            case TEXT:
            case JSON_OBJECT:
                return "text";//长文本
            case BYTE_ARRAY:
            case FILE:
                return "VarBinary(MAX)";
            case FILE_ID:
                return "varchar(64)";//默认记录文件的ID号
            case ENUM_NAME:
                return "varchar(64)";//
            default:
                return ft;
        }
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
        switch (ft) {
            case STRING:
                return "varchar";
            case IDENTITY:
                return "INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1 )";
            case INTEGER:
            case LONG:
                return "INTEGER";
            case DOUBLE:
            case FLOAT:
                return "DECIMAL";
            case MONEY:
                return "DECIMAL(30,4)";
            case BOOLEAN:
                return "varchar(1)";
            case DATE:
            case DATETIME:
                return "Date";
            case TIMESTAMP:
                return "TimeStamp";
            case TEXT:
            case JSON_OBJECT:
                return "clob(52428800)";//长文本
            case BYTE_ARRAY:
            case FILE:
                return "BLOB";
            case FILE_ID:
                return "varchar(64)";//默认记录文件的ID号
            case ENUM_NAME:
                return "varchar(64)";//
            default:
                return ft;
        }
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
        switch (ft) {
            case STRING:
                return "varchar";
            case INTEGER:
                return "INT";
            case IDENTITY:
                return "bigint AUTO_INCREMENT";
            case LONG:
                return "BIGINT";
            case MONEY:
                return "DECIMAL(30,4)";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case BOOLEAN:
                return "varchar(1)";
            case DATE:
                return "Date";
            case DATETIME:
                return "DATETIME";
            case TIMESTAMP:
                return "TimeStamp";
            case TEXT:
            case JSON_OBJECT:
                return "LONGTEXT";//长文本
            case FILE_ID:
                return "varchar(64)";//默认记录文件的ID号
            case BYTE_ARRAY:
            case FILE:
                return "VARBINARY";
            case ENUM_NAME:
                return "varchar(64)";//
            default:
                return ft;
        }
    }

    public static String mapToClickHouseColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        switch (ft) {
            case STRING:
                return "String";
            case INTEGER:
                return "Int32";
            case IDENTITY:
            case LONG:
                return "Int64";
            case MONEY:
                return "Decimal32(4)";
            case FLOAT:
                return "Float32";
            case DOUBLE:
                return "Float64";
            case BOOLEAN:
                return "FixedString(1)";
            case DATE:
                return "Date";
            case DATETIME:
                return "Datetime";
            case TIMESTAMP:
                return "Datetime64";
            case TEXT:
            case JSON_OBJECT:
            case BYTE_ARRAY:
            case FILE:
                return "String";//长文本
            case FILE_ID:
                return "FixedString(64)";//默认记录文件的ID号
            case ENUM_NAME:
                return "FixedString(64)";//
            default:
                return ft;
        }
    }

    public static String mapToPostgreSqlColumnType(String ft) {
        if (StringUtils.isBlank(ft))
            return ft;
        switch (ft) {
            case STRING:
                return "varchar";
            case INTEGER:
                return "integer";
            case IDENTITY:
                return "SERIAL";
            case LONG:
                return "bigint";
            case MONEY:
                return "money";
            case FLOAT:
            case DOUBLE:
                return "decimal";
            case BOOLEAN:
                return "char(1)";
            case DATE:
                return "Date";
            case DATETIME:
            case TIMESTAMP:
                return "TimeStamp";
            case TEXT:
            case JSON_OBJECT:
                return "TEXT";//长文本
            case FILE_ID:
                return "varchar(64)";//默认记录文件的ID号
            case BYTE_ARRAY:
            case FILE:
                return "character";
            case ENUM_NAME:
                return "varchar(64)";//
            default:
                return ft;
        }
    }

    /**
     * @param dt 数据库类别
     * @param ft 字段 java 类别
     * @return String
     */
    public static String mapToDatabaseType(String ft, DBType dt) {
        if (dt == null || StringUtils.isBlank(ft))
            return ft;
        switch (dt) {
            case SqlServer:
                return mapToSqlServerColumnType(ft);
            case DB2:
                return mapToDB2ColumnType(ft);
            case H2:
            case MySql:
                return mapToMySqlColumnType(ft);
            case ClickHouse:
                return mapToClickHouseColumnType(ft);
            case PostgreSql:
                return mapToPostgreSqlColumnType(ft);
            case GBase:
                return mapToGBaseColumnType(ft);
            case Oracle:
            case DM:
            case KingBase:
            case Oscar:
            default:
                return mapToOracleColumnType(ft);
        }

    }

    public static Map<String, String> getAllTypeMap() {
        Map<String, String> fts = new HashMap<>();
        fts.put(FieldType.STRING, "字符串");
        fts.put(FieldType.INTEGER, "整型");
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
        if ("NUMBER".equalsIgnoreCase(columnType) ||
            "DECIMAL".equalsIgnoreCase(columnType)) {
            if (scale > 0) {
                return BigDecimal.class;
            } else {
                return Long.class;
            }
        } else if ("CHAR".equalsIgnoreCase(columnType) ||
            "VARCHAR".equalsIgnoreCase(columnType) ||
            "VARCHAR2".equalsIgnoreCase(columnType) ||
            "CLOB".equalsIgnoreCase(columnType) ||
            "TEXT".equalsIgnoreCase(columnType) ||
            "FixedString".equalsIgnoreCase(columnType) ||
            FieldType.STRING.equalsIgnoreCase(columnType) ||
            FieldType.FILE_ID.equalsIgnoreCase(columnType)) {
            return String.class;
        } else if ("DATE".equalsIgnoreCase(columnType) ||
            "TIME".equalsIgnoreCase(columnType) ||
            "DATETIME".equalsIgnoreCase(columnType) ||
            "SQLDATE".equalsIgnoreCase(columnType)) {
            return Date.class;
        } else if ("TIMESTAMP".equalsIgnoreCase(columnType) ||
            "DATETIME64".equalsIgnoreCase(columnType) ||
            "SQLTIMESTAMP".equalsIgnoreCase(columnType)) {
            return Timestamp.class;
        } else if ("BLOB".equalsIgnoreCase(columnType) ||
            "VARBINARY".equalsIgnoreCase(columnType) ||
            FieldType.BYTE_ARRAY.equalsIgnoreCase(columnType) ||
            "JSONB".equalsIgnoreCase(columnType)) {
            return byte[].class;
        } else if (FieldType.MONEY.equalsIgnoreCase(columnType) ||
                "DECIMAL32".equalsIgnoreCase(columnType) ||
                "DECIMAL64".equalsIgnoreCase(columnType) ||
                "DECIMAL128".equalsIgnoreCase(columnType)) {
            return BigDecimal.class;//FieldType.MONEY;
        } else if ("Float32".equalsIgnoreCase(columnType) ||
            FieldType.FLOAT.equalsIgnoreCase(columnType)) {
            return Float.class;
        } else if ("INT".equalsIgnoreCase(columnType) ||
            "INT4".equalsIgnoreCase(columnType) ||
            "INT8".equalsIgnoreCase(columnType) ||
            "INT16".equalsIgnoreCase(columnType) ||
            "INT32".equalsIgnoreCase(columnType) ||
            "UINT16".equalsIgnoreCase(columnType) ||
            "UINT32".equalsIgnoreCase(columnType) ||
            FieldType.INTEGER.equalsIgnoreCase(columnType)) {
            return Integer.class;
        } else if ("Float64".equalsIgnoreCase(columnType) ||
            FieldType.DOUBLE.equalsIgnoreCase(columnType)) {
            return Double.class;
        } else if ("BIGINT".equalsIgnoreCase(columnType) ||
            "INT64".equalsIgnoreCase(columnType) ||
            "UINT64".equalsIgnoreCase(columnType) ||
            FieldType.LONG.equalsIgnoreCase(columnType)) {
            return Long.class;
        } else if (FieldType.JSON_OBJECT.equalsIgnoreCase(columnType) ||
            "JSON".equalsIgnoreCase(columnType) ) {
            return JSON.class;
        } else if ("bool".equalsIgnoreCase(columnType) ||
            "boolean".equalsIgnoreCase(columnType)) {
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
        switch (dbType) {
            case -6:
            case -5:
            case 5:
            case 4:
            case 2:
                return Integer.class;
            case 6:
            case 7:
                return Float.class;
            case 8:
                return Double.class;
            case 3:
                return Long.class;

            case 91:
            case 92:
                return Date.class;
            case 93:
            case 2013:
            case 2014:
                return Timestamp.class;
            case -2:
            case -3:
            case -4:
            case 2004:
                return byte[].class;
            case -1:
            case 1:
            case 12:
            default:
                return String.class;
        }
    }

    public static String mapToFieldType(int dbType) {
        switch (dbType) {
            case -6:
            case -5:
            case 5:
            case 4:
            case 2:
                return FieldType.INTEGER;
            case 6:
            case 7:
                return FieldType.FLOAT;
            case 8:
                return FieldType.DOUBLE;
            case 3:
                return FieldType.LONG;

            case 91:
                return FieldType.DATE;
            case 92:
                return FieldType.DATETIME;
            case 93:
            case 2013:
            case 2014:
                return FieldType.TIMESTAMP;
            case -2:
            case -3:
            case -4:
            case 2004:
                return FieldType.BYTE_ARRAY;
            case 2005:
                return FieldType.TEXT;
            case 16:
                return FieldType.BOOLEAN;
            case -1:
            case 1:
            case 12:
            default:
                return FieldType.STRING;
        }
    }

    public static String mapToFieldType(String columnType, int scale) {
        if ("NUMBER".equalsIgnoreCase(columnType) ||
            "NUMERIC".equalsIgnoreCase(columnType) ||
            "DECIMAL".equalsIgnoreCase(columnType)) {
            if (scale > 0) {
                return FieldType.DOUBLE;
            } else {
                return FieldType.LONG;
            }
        } else if ("CHAR".equalsIgnoreCase(columnType) ||
            "VARCHAR".equalsIgnoreCase(columnType) ||
            "VARCHAR2".equalsIgnoreCase(columnType)) {
            return FieldType.STRING;
        } else if ("DATE".equalsIgnoreCase(columnType) ||
            "SQLDATE".equalsIgnoreCase(columnType) ){
            return FieldType.DATE;
        } else if("TIME".equalsIgnoreCase(columnType) ||
            "DATETIME".equalsIgnoreCase(columnType)) {
            return FieldType.DATETIME;
        } else if ("TIMESTAMP".equalsIgnoreCase(columnType) ||
            "SQLTIMESTAMP".equalsIgnoreCase(columnType)) {
            return FieldType.TIMESTAMP;
        } else if ("CLOB".equalsIgnoreCase(columnType) ||
            "TEXT".equalsIgnoreCase(columnType)) {
            return FieldType.TEXT;
        } else if ("BLOB".equalsIgnoreCase(columnType) ||
            "VARBINARY".equalsIgnoreCase(columnType)) {
            return FieldType.BYTE_ARRAY;
        } else if ("FLOAT".equalsIgnoreCase(columnType)) {
            return FieldType.FLOAT;
        } else if ("DOUBLE".equalsIgnoreCase(columnType)) {
            return FieldType.DOUBLE;
        } else  if ("BIGINT".equalsIgnoreCase(columnType) ||
            "INT64".equalsIgnoreCase(columnType) ||
            "UINT64".equalsIgnoreCase(columnType) ||
            "SERIAL".equalsIgnoreCase(columnType)) {
            return FieldType.LONG;
        } else if ("INT".equalsIgnoreCase(columnType) ||
            "INT4".equalsIgnoreCase(columnType) ||
            "INT8".equalsIgnoreCase(columnType) ||
            "INT16".equalsIgnoreCase(columnType) ||
            "INT32".equalsIgnoreCase(columnType) ||
            "UINT16".equalsIgnoreCase(columnType) ||
            "UINT32".equalsIgnoreCase(columnType) ||
            "INTEGER".equalsIgnoreCase(columnType) ) {
            return FieldType.INTEGER;
        } else if ("bool".equalsIgnoreCase(columnType) ||
            "boolean".equalsIgnoreCase(columnType)) {
            return FieldType.BOOLEAN;
        }

        return columnType;
    }

    public static String mapToFieldType(String columnType) {
        return mapToFieldType(columnType, 0);
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
            return FieldType.DATE;
        }

        if (byte[].class == javaType) {
            return FieldType.BYTE_ARRAY;
        }

        return FieldType.JSON_OBJECT;
    }

    public static String mapToSqliteColumnType(String javaType) {
        switch (javaType){
            case FieldType.INTEGER:
            case FieldType.LONG:
                return "INTEGER";
            case FieldType.MONEY:
                return "DECIMAL(20,4)";
            case FieldType.DOUBLE:
            case FieldType.FLOAT:
                return "REAL";
            case FieldType.TIMESTAMP:
            case FieldType.DATE:
                return "DATETIME";
            case FieldType.BYTE_ARRAY:
                return "BLOB";
            case FieldType.STRING:
            default:
                return "TEXT";
        }

    }
}
