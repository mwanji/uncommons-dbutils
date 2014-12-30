package com.moandjiezana.uncommons.dbutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/**
 * Executes SQL queries with pluggable strategies for handling {@link ResultSet}s.
 * 
 * @see ResultSetHandler
 */
public interface QueryRunner {

  /**
   * <pre><code>
   * try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
   *   QueryRunner queryRunner = QueryRunner.create(connection);
   * }
   * </code></pre>
   * 
   * @param connection
   *    the {@link Connection} to be used. Must be managed by the caller
   * @return a {@link Connection}-based QueryRunner
   */
  static QueryRunner create(Connection connection) {
    return new ConnectionQueryRunner(connection);
  }
  
  /**
   * @param sql
   *    the SELECT to execute
   * @param resultSetHandler
   *    transforms the {@link ResultSet}
   * @param params
   *    values for the SQL placeholders
   * @param <T>
   *    the type of instance to return
   * @return an instance of T as determined by resultSetHandler
   * @throws Exception
   *    if anything goes wrong
   */
  <T> T select(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception;

  /**
   * @param sql
   *    the UPDATE, DELETE or DDL to execute. Ignores any values returned by the {@link ResultSet}.
   * @param params
   *    values for the SQL placeholders
   *
   * @return The number of rows updated.
   * @throws Exception
   *    if anything goes wrong
   */
  int execute(String sql, Object... params) throws Exception;

  /**
   * @param sql
   *    the INSERT to execute
   * @param resultSetHandler
   *    transforms the {@link ResultSet}, which contains the generated keys
   * @param params
   *    values for the SQL placeholders
   * @param <T>
   *    the type of instance to return
   * @return an instance of T as determined by resultSetHandler
   * @throws Exception
   *    if anything goes wrong
   */
  <T> T insert(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception;
  
  /**
   * @param sql
   *    the SQL to execute
   * @param params
   *    values for the SQL placeholders. Eg. <code>Arrays.asList(Arrays.asList(1L), Arrays.asList(2L))</code> will create a batch of 2 queries, using 1, then 2, as values.
   * @return the number of affected rows for each time the SQL was executed
   * @throws Exception
   *    if anything goes wrong
   */
  int[] batch(String sql, List<List<Object>> params) throws Exception;
  
  /**
   * @param sql
   *    The INSERT to execute
   * @param resultSetHandler
   *    transforms the {@link ResultSet}, which contains the generated keys
   * @param params
   *    values for the SQL placeholders. Eg. <code>Arrays.asList(Arrays.asList(1L), Arrays.asList(2L))</code> will create a batch of 2 queries, using 1, then 2, as values.
   * @param <T>
   *    the type of instance to return
   * @return an instance of T as determined by resultSetHandler
   * @throws Exception
   *    if anything goes wrong
   */
  <T> T batchInsert(String sql, ResultSetHandler<T> resultSetHandler, List<List<Object>> params) throws Exception;
}
