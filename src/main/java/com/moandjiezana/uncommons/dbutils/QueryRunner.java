/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.moandjiezana.uncommons.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import javax.sql.DataSource;

import com.moandjiezana.uncommons.dbutils.functions.BiConsumerWithException;
import com.moandjiezana.uncommons.dbutils.functions.ConsumerWithException;
import com.moandjiezana.uncommons.dbutils.functions.FunctionWithException;
import com.moandjiezana.uncommons.dbutils.functions.SupplierWithException;

/**
 * Executes SQL queries with pluggable strategies for handling {@link ResultSet}s.
 * Immutable, but only as threadsafe as the underlying {@link Connection}. Avoid sharing {@link Connection} instances across threads!
 * 
 * @see ResultSetHandler
 */
public class QueryRunner {
  
  public static class Transaction {
    public void commit() throws SQLException {
      connection.commit();
    }
    
    public void rollback() throws SQLException {
      connection.rollback();
    }
    
    private final Connection connection;

    private Transaction(Connection connection) {
      this.connection = connection;
    }
  }

  private final SupplierWithException<Connection> connection;
  private final ConsumerWithException<Connection> finalizer;

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
  public static QueryRunner create(Connection connection) {
    return new QueryRunner(() -> connection, c -> {});
  }

  public static QueryRunner create(DataSource dataSource) {
    return new QueryRunner(() -> dataSource.getConnection(), c -> c.close());
  }
  
  public QueryRunner initializeWith(ConsumerWithException<Connection> initializer) {
    return new QueryRunner(connection.andThen(initializer), finalizer);
  }

  public AsyncQueryRunner toAsync() {
    return toAsync(ForkJoinPool.commonPool());
  }

  public AsyncQueryRunner toAsync(Executor executor) {
    return new AsyncQueryRunner(this, executor);
  }
  
  /**
   * <p>
   * SQL NOTE:
   * If you wish to use possibly <code>null</code> values in a WHERE clause's params, do not use "= ?", as in SQL <code>NULL = NULL</code> and <code>NULL &lt;&gt; NULL</code> will not return <code>true</code>.
   * Use <code>IS DISTINCT FROM ?</code> or <code>IS NOT DISTINCT FROM ?</code> instead.
   * </p>
   * 
   * <p>
   * Known to be supported in:
   * </p>
   * <ul>
   *  <li><a href="https://wiki.postgresql.org/wiki/Is_distinct_from">PostgreSQL</a></li>
   *  <li><a href="http://h2database.com/html/grammar.html#condition_right_hand_side">H2</a> (also aliased to <code>IS [NOT] ?</code>)</li>
   * </ul>
   * 
   * <p>
   * Alternatives:
   * </p>
   * <ul>
   *  <li><a href="http://dev.mysql.com/doc/refman/5.6/en/comparison-operators.html#operator_equal-to">MySQL</a> uses <code>&lt;=&gt;</code></li>
   *  <li><a href="https://mariadb.com/kb/en/mariadb/documentation/functions-and-operators/operators/comparison-operators/null-safe-equal/">MariaDB</a> uses <code>&lt;=&gt;</code></li>
   *  <li><a href="http://www.sqlite.org/lang_expr.html#binaryops">SQLite</a> uses <code>IS [NOT] ?</code></li>
   * </ul>
   * 
   * <p>
   * See <a href="http://blog.jooq.org/2012/09/21/the-is-distinct-from-predicate/">http://blog.jooq.org/2012/09/21/the-is-distinct-from-predicate/</a> for more info.
   * </p>
   * 
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
 public <T> T select(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception {
    return run(c -> {
      try (PreparedStatement stmt = c.prepareStatement(sql);) {
        fillStatementParam(stmt, params);
        
        try (ResultSet rs = stmt.executeQuery();) {
          return resultSetHandler.handle(rs);
        }
      }
    });
  }

  /**
   * <p>SQL NOTE: if you wish to use possibly <code>null</code> values, please see the discussion in {@link #select(String, ResultSetHandler, Object...)}</p>
   * 
   * @param sql
   *    the UPDATE, DELETE or DDL to execute. Ignores any values returned by the {@link ResultSet}.
   * @param params
   *    values for the SQL placeholders
   *
   * @return The number of rows updated.
   * @throws Exception
   *    if anything goes wrong
   */
  public int execute(String sql, Object... params) throws Exception {
    return run(c -> {
      try (PreparedStatement statement = c.prepareStatement(sql);) {
        fillStatementParam(statement, params);

        return statement.executeUpdate();
      }
    });
  }
  
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
  public <T> T insert(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception {
    return run(c -> {
    try (PreparedStatement stmt = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
      fillStatementParam(stmt, params);
      stmt.executeUpdate();
      
      try (ResultSet resultSet = stmt.getGeneratedKeys();) {
        return resultSetHandler.handle(resultSet);
      }
    }
    });
  }

  /**
   * @param sql
   *    the SQL to execute
   * @param params
   *    values for the SQL placeholders. Eg. <code>Arrays.asList(Arrays.asList(1L), Arrays.asList(2L))</code> will create a batch of 2 queries, using 1, then 2, as values.
   * @return the number of affected rows for each time the SQL was executed
   * @throws Exception
   *    if anything goes wrong
   */
  public int[] batch(String sql, List<List<Object>> params) throws Exception {
    return run(c -> {
      try (PreparedStatement statement = c.prepareStatement(sql);) {
        for (int i = 0; i < params.size(); i++) {
          this.fillStatementParams(statement, params.get(i));
          statement.addBatch();
        }

        return statement.executeBatch();
      }
    });
  }

  /**
   * @param sql
   *    The INSERT to execute
   * @param resultSetHandler
   *    transforms the {@link ResultSet}, which contains the generated keys
   * @param batchParams
   *    values for the SQL placeholders. Eg. <code>Arrays.asList(Arrays.asList(1L), Arrays.asList(2L))</code> will create a batch of 2 queries, using 1, then 2, as values.
   * @param <T>
   *    the type of instance to return
   * @return an instance of T as determined by resultSetHandler
   * @throws Exception
   *    if anything goes wrong
   */
  public <T> T batchInsert(String sql, ResultSetHandler<T> resultSetHandler, List<List<Object>> batchParams) throws Exception {
    return run(c -> {
      try (PreparedStatement stmt = c.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
        for (List<Object> params : batchParams) {
          this.fillStatementParams(stmt, params);
          stmt.addBatch();
        }
        stmt.executeBatch();
        ResultSet rs = stmt.getGeneratedKeys();

        return resultSetHandler.handle(rs);
      }
    });
  }
  
  /**
   * <p>Provides a {@link QueryRunner} that can be used in a transaction.</p>
   * 
   * <p>The underlying {@link Connection} is configured in the same way as any other {@link Connection} from this {@link QueryRunner}, except that
   * {@link Connection#setAutoCommit(boolean)} is set to false before any calls are made and set back to its previous state afterwards.</p>
   * 
   * <p>
   * Make sure to call {@link Connection#rollback()} or {@link Connection#commit()} if necessary!
   * As {@link Connection#setAutoCommit(boolean)} is called after the transaction is complete, if it is set to <code>true</code>,
   * then any pending queries will be committed. Also, leaving pending transactions could have side-effects when the {@link Connection} is reused.
   * </p>
   * 
   * @param txQueryRunner
   *    Make sure to use the {@link QueryRunner} passed to this lamba!
   * @throws Exception
   *    if anything goes wrong
   */
  public void tx(BiConsumerWithException<QueryRunner, QueryRunner.Transaction> txQueryRunner) throws Exception {
    Connection _connection = connection.get();
    boolean originalAutoCommit = _connection.getAutoCommit();
    _connection.setAutoCommit(false);
    QueryRunner queryRunner = new QueryRunner(() -> _connection, c -> {});
    try {
      txQueryRunner.accept(queryRunner, new QueryRunner.Transaction(_connection));
    } finally {
      finalizer.accept(_connection);
      _connection.setAutoCommit(originalAutoCommit);
    }
  }

  QueryRunner(SupplierWithException<Connection> connection, ConsumerWithException<Connection> finalizer) {
    this.connection = connection;
    this.finalizer = finalizer;
  }

  private void fillStatementParams(PreparedStatement statement, List<Object> params) throws SQLException {
    for (int i = 0; i < params.size(); i++) {
      Object param = params.get(i);
      int jdbcIndex = i + 1;
      fillStatementParam(statement, param, jdbcIndex);
    }
  }
  
  private void fillStatementParam(PreparedStatement statement, Object[] params) throws SQLException {
    for (int i = 0; i < params.length; i++) {
      Object param = params[i];
      int jdbcIndex = i + 1;
      fillStatementParam(statement, param, jdbcIndex);
    }
  }

  private void fillStatementParam(PreparedStatement statement, Object param, int jdbcIndex) throws SQLException {
    if (param != null) {
      statement.setObject(jdbcIndex, param);
    } else {
      // VARCHAR works with many drivers regardless
      // of the actual column type. Oddly, NULL and
      // OTHER don't work with Oracle's drivers.
      int sqlType = Types.VARCHAR;
      sqlType = statement.getParameterMetaData().getParameterType(jdbcIndex);
      statement.setNull(jdbcIndex, sqlType);
    }
  }
  
  private <T> T run(FunctionWithException<Connection, T> consumer) throws Exception {
    Connection c = connection.get();
    try {
      return consumer.apply(c);
    } finally {
      finalizer.accept(c);
    }
  }
}
