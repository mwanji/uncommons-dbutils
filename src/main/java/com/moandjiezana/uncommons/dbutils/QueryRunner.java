package com.moandjiezana.uncommons.dbutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/**
 * Executes SQL queries with pluggable strategies for handling {@link ResultSet}s.
 */
public interface QueryRunner {

  static QueryRunner create(Connection connection) {
    return new ConnectionQueryRunner(connection);
  }
  
  <T> T select(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception;

  /**
   * Executes the given UPDATE or DELETE SQL statement. This
   * <code>Connection</code> must be in auto-commit mode or the update will not
   * be saved.
   *
   * @return The number of rows updated.
   */
  int execute(String sql, Object... params) throws Exception;

  /**
   * The generated keys are given to the resultSetHandler.
   */
  <T> T insert(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception;
  int[] batch(String sql, List<List<Object>> params) throws Exception;
  <T> T batch(String sql, ResultSetHandler<T> resultSetHandler, List<List<Object>> params) throws Exception;
}
