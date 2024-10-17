package com.centit.support.database.jsonmaptable;

import com.centit.support.database.metadata.TableInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class SqliteJsonObjectDao extends GeneralJsonObjectDao {

    public SqliteJsonObjectDao() {

    }

    public SqliteJsonObjectDao(Connection conn) {
        super(conn);
    }

    public SqliteJsonObjectDao(TableInfo tableInfo) {
        super(tableInfo);
    }

    public SqliteJsonObjectDao(Connection conn, TableInfo tableInfo) {
        super(conn, tableInfo);
    }

    @Override
    public Long getSequenceNextValue(String sequenceName) throws SQLException, IOException {
        return null;
    }
}
