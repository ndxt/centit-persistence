package com.centit.framework.jdbc.dao;

import com.centit.support.database.jsonmaptable.JsonObjectDao;

import java.io.IOException;
import java.sql.SQLException;

public interface JsonDaoExecuteWork<T> {
    T execute(JsonObjectDao conn) throws SQLException, IOException;
}
