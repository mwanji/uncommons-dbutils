package com.moandjiezana.uncommons.dbutils;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

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
  public <T> T batch(String sql, ResultSetHandler<T> resultSetHandler, List<List<Object>> params) throws Exception {
    return run(qr -> qr.batch(sql, resultSetHandler, params));
  }
  
  private <T> T run(QueryFunction<T> f) throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      return f.apply(QueryRunner.create(connection));
    }
  }
  
  private static interface QueryFunction<R> {
    R apply(QueryRunner queryRunner) throws Exception;
  }
}
