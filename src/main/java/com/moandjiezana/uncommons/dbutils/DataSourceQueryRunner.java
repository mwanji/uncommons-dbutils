package com.moandjiezana.uncommons.dbutils;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

import com.moandjiezana.uncommons.dbutils.functions.FunctionWithException;

/*
 * Each method opens and closes a Connection taken from the DataSource
 */
class DataSourceQueryRunner implements QueryRunner {
  
  private final DataSource dataSource;

  DataSourceQueryRunner(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public <T> T select(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception {
    return run(qr -> qr.select(sql, resultSetHandler, params));
  }
  
  @Override
  public int execute(String sql, Object... params) throws Exception {
    return run(qr -> qr.execute(sql, params));
  }

  @Override
  public <T> T insert(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception {
    return run(qr -> qr.insert(sql, resultSetHandler, params));
  }

  @Override
  public int[] batch(String sql, List<List<Object>> params) throws Exception {
    return run(qr -> qr.batch(sql, params));
  }

  @Override
  public <T> T batchInsert(String sql, ResultSetHandler<T> resultSetHandler, List<List<Object>> params) throws Exception {
    return run(qr -> qr.batchInsert(sql, resultSetHandler, params));
  }
  
  private <T> T run(FunctionWithException<QueryRunner, T> f) throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      return f.apply(QueryRunner.create(connection));
    }
  }
}
