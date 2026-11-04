package com.centit.support.database.metadata;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.sql.*;
import java.util.*;

/** Unbounded structural inspection. Unknown or lossy definitions fail closed. */
public final class JdbcBaselineReader {
    private JdbcBaselineReader() {
    }

    /** Canonical JDBC table kind ("TABLE"/"VIEW") for a single table, or null when absent. */
    public static String tableType(Connection connection, String schema, String table) throws SQLException {
        JSONObject identity = JdbcTableVerification.resolveIdentity(connection, schema, table);
        return identity == null ? null : tableType(identity.getString("tableType"));
    }

    /** JDBC table kinds are normalized to the standard words; unknown kinds are not ordinary tables. */
    public static String tableType(String nativeType) {
        return switch (key(nativeType)) {
            case "TABLE", "BASE TABLE" -> "TABLE";
            case "VIEW" -> "VIEW";
            default -> throw new IllegalArgumentException("Unsupported native table type");
        };
    }

    /** JDBC visibility alone is not proof of absence; permission errors preserve metadata. */
    public static void assertTableAbsent(Connection connection, String schema, String table) throws SQLException {
        String quote = connection.getMetaData().getIdentifierQuoteString().trim();
        if (quote.isEmpty()) throw new SQLException("Cannot verify missing table safely");
        String name = quote + table.replace(quote, quote + quote) + quote;
        if (schema != null && !schema.isBlank()) name = quote + schema.replace(quote, quote + quote) + quote + "." + name;
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery("select * from " + name + " where 1=0").close();
        } catch (SQLException error) {
            String state = error.getSQLState();
            String product = connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
            if (product.contains("h2") && (error.getErrorCode() == 42102 || error.getErrorCode() == 42104)) return;
            if (!product.contains("oracle") && ("42S02".equals(state) || "42P01".equals(state))) return;
            throw error;
        }
        throw new SQLException("Table exists but JDBC metadata is incomplete: " + table);
    }

    public static JSONArray primaryKeys(Connection c, String catalog, String schema, String table) throws SQLException {
        TreeMap<Integer, JSONObject> keys = new TreeMap<>();
        Set<String> columns = new HashSet<>();
        try (ResultSet rows = c.getMetaData().getPrimaryKeys(catalog, schema, table)) {
            while (rows.next()) {
                String name = rows.getString("COLUMN_NAME");
                int sequence = rows.getInt("KEY_SEQ");
                if (rows.wasNull() || sequence <= 0 || name == null || name.isBlank()
                        || !columns.add(key(name)) || keys.containsKey(sequence))
                    throw new SQLException("Primary-key sequence is incomplete: " + table);
                keys.put(sequence, JSONObject.of("columnName", name, "sequence", sequence, "name", rows.getString("PK_NAME")));
            }
        }
        for (int sequence = 1; sequence <= keys.size(); sequence++)
            if (!keys.containsKey(sequence)) throw new SQLException("Primary-key sequence is incomplete: " + table);
        return new JSONArray(keys.values());
    }

    public static JSONArray indexes(Connection c, String catalog, String schema, String table,
                                    JSONArray primaryKeys) throws SQLException {
        String product = c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        Map<String, JSONObject> indexes = new TreeMap<>();
        Map<String, TreeMap<Integer,String>> fields = new HashMap<>();
        Set<String> primaryNames = new HashSet<>();
        for (Object value : primaryKeys) primaryNames.add(key(((JSONObject) value).getString("name")));
        try (ResultSet r = c.getMetaData().getIndexInfo(catalog, schema, table, false, false)) {
            while (r.next()) {
                if (r.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) continue;
                String name = r.getString("INDEX_NAME");
                if (name == null) throw new SQLException("Index name unavailable: " + table);
                String key = key(name);
                if (primaryNames.contains(key) || "PRIMARY".equals(key)
                        || (product.contains("h2") && key.startsWith("PRIMARY_KEY_"))) continue;
                String column = r.getString("COLUMN_NAME");
                String filter = r.getString("FILTER_CONDITION");
                String order = r.getString("ASC_OR_DESC");
                if (column == null || (filter != null && !filter.isBlank()) || "D".equalsIgnoreCase(order))
                    throw new SQLException("Index cannot be captured without losing expression/filter/order: " + name);
                JSONObject index = indexes.get(key);
                if (index == null) {
                    index = new JSONObject(); index.put("indexName", name);
                    index.put("indexType", r.getBoolean("NON_UNIQUE") ? "NORMAL" : "UNIQUE");
                    indexes.put(key, index); fields.put(key, new TreeMap<>());
                }
                int position = r.getInt("ORDINAL_POSITION");
                if (position < 1 || fields.get(key).put(position, column) != null)
                    throw new SQLException("Incomplete index order: " + name);
            }
        }
        if (product.contains("mysql") || product.contains("mariadb")) {
            try (PreparedStatement ps = c.prepareStatement("select INDEX_NAME, INDEX_TYPE, INDEX_COMMENT, SUB_PART from information_schema.STATISTICS where TABLE_SCHEMA=? and TABLE_NAME=?")) {
                ps.setString(1, catalog); ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) { while (rs.next()) {
                    String name = key(rs.getString("INDEX_NAME"));
                    JSONObject index = indexes.get(name);
                    if (index == null) continue;
                    if (rs.getObject("SUB_PART") != null) throw new SQLException("Prefix index cannot be captured completely: " + name);
                    String type = rs.getString("INDEX_TYPE");
                    if ("FULLTEXT".equalsIgnoreCase(type)) throw new SQLException("Fulltext parser cannot be captured completely: " + name);
                    if ("HASH".equalsIgnoreCase(type)) index.put("indexType", "HASH");
                    else if (!"BTREE".equalsIgnoreCase(type)) throw new SQLException("Unsupported index type: " + type);
                    index.put("indexComment", rs.getString("INDEX_COMMENT"));
                }}
            }
        }
        if (!indexes.isEmpty()) validateDialectIndexes(c, schema, table, product);
        JSONArray result = new JSONArray();
        for (var entry : indexes.entrySet()) {
            entry.getValue().put("indexFields", new ArrayList<>(fields.get(entry.getKey()).values()));
            result.add(entry.getValue());
        }
        return result;
    }

    private static void validateDialectIndexes(Connection c, String schema, String table, String product) throws SQLException {
        String sql;
        if (product.contains("mysql") || product.contains("mariadb")) {
            String quoted = "`" + table.replace("`", "``") + "`";
            try (Statement statement = c.createStatement(); ResultSet result = statement.executeQuery("show create table " + quoted)) {
                if (!result.next()) throw new SQLException("Complete index definition unavailable: " + table);
                String definition = result.getString(2).toUpperCase(Locale.ROOT);
                if (definition.contains("INVISIBLE") || definition.contains("IGNORED"))
                    throw new SQLException("Invisible/ignored index cannot be captured: " + table);
            }
            return;
        }
        if (product.contains("h2")) return;
        if (product.contains("postgres") || product.contains("kingbase")) {
            sql = "select count(*) from pg_index i join pg_class t on t.oid=i.indrelid join pg_namespace n on n.oid=t.relnamespace join pg_class x on x.oid=i.indexrelid join pg_am a on a.oid=x.relam where n.nspname=? and t.relname=? and not i.indisprimary and (i.indpred is not null or i.indexprs is not null or i.indnkeyatts<>i.indnatts or a.amname<>'btree' or not i.indisvalid)";
        } else if (product.contains("microsoft")) {
            sql = "select count(*) from sys.indexes i join sys.tables t on t.object_id=i.object_id join sys.schemas s on s.schema_id=t.schema_id where s.name=? and t.name=? and i.is_primary_key=0 and i.type<>0 and (i.type<>2 or i.has_filter=1 or i.is_disabled=1 or exists(select 1 from sys.index_columns ic where ic.object_id=i.object_id and ic.index_id=i.index_id and ic.is_included_column=1))";
        } else if (product.contains("oracle")) {
            sql = "select count(*) from ALL_INDEXES where TABLE_OWNER=? and TABLE_NAME=? and (INDEX_TYPE<>'NORMAL' or PARTITIONED='YES' or VISIBILITY<>'VISIBLE')";
        } else throw new SQLException("Complete index capture is not supported for " + product);
        try (PreparedStatement statement = c.prepareStatement(sql)) {
            statement.setString(1, schema); statement.setString(2, table);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || result.getInt(1) != 0) throw new SQLException("Special index cannot be captured completely: " + table);
            }
        }
    }

    public static String viewSql(Connection c, String catalog, String schema, String table) throws SQLException {
        String sql;
        String product = c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        String owner = schema;
        if (product.contains("mysql") || product.contains("mariadb")) {
            sql = "select VIEW_DEFINITION from information_schema.VIEWS where TABLE_SCHEMA=? and TABLE_NAME=?";
            owner = catalog;
        } else if (product.contains("h2")) {
            sql = "select VIEW_DEFINITION from information_schema.VIEWS where TABLE_SCHEMA=? and TABLE_NAME=?";
        } else if (product.contains("postgres") || product.contains("kingbase")) {
            sql = "select pg_get_viewdef(c.oid,true) from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname=? and c.relname=? and c.relkind='v'";
        } else if (product.contains("oracle") || product.equals("dm dbms")) {
            sql = "select TEXT from ALL_VIEWS where OWNER=? and VIEW_NAME=?";
        } else if (product.contains("microsoft")) {
            sql = "select m.definition from sys.views v join sys.schemas s on s.schema_id=v.schema_id join sys.sql_modules m on m.object_id=v.object_id where s.name=? and v.name=?";
        } else if (product.contains("db2")) {
            sql = "select TEXT from SYSCAT.VIEWS where VIEWSCHEMA=? and VIEWNAME=?";
        } else throw new SQLException("View baseline is not supported for " + product);
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, owner); ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new SQLException("View definition unavailable: " + table);
                String definition = rs.getString(1);
                if (definition == null || definition.isBlank() || rs.next()) throw new SQLException("View definition unavailable or ambiguous: " + table);
                return viewQuery(definition);
            }
        }
    }

    /** Extract only a top-level AS, never one in identifiers, comments or string literals. */
    public static String viewQuery(String definition) throws SQLException {
        String text = definition.trim();
        if (!text.regionMatches(true, 0, "CREATE", 0, 6)) return text.replaceFirst(";\\s*$", "");
        int depth = 0; char quote = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == quote) { if (i+1 < text.length() && text.charAt(i+1)==quote) i++; else quote=0; }
                continue;
            }
            if (c=='\'' || c=='"' || c=='`' || c=='[') { quote=c=='['?']':c; continue; }
            if (c=='-' && i+1<text.length() && text.charAt(i+1)=='-') { int n=text.indexOf('\n',i+2); if(n<0) break; i=n; continue; }
            if (c=='/' && i+1<text.length() && text.charAt(i+1)=='*') { int n=text.indexOf("*/",i+2); if(n<0) break; i=n+1; continue; }
            if(c=='(') depth++; else if(c==')') depth--;
            if(depth==0 && i>0 && Character.isWhitespace(text.charAt(i-1)) && text.regionMatches(true,i,"AS",0,2)
                    && i+2<text.length() && Character.isWhitespace(text.charAt(i+2))) return text.substring(i+2).trim().replaceFirst(";\\s*$", "");
        }
        throw new SQLException("Cannot extract complete view definition");
    }

    private static String key(String name) {
        return name == null ? "" : name.trim().toUpperCase(Locale.ROOT);
    }
}
