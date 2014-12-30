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
import java.util.Arrays;
import java.util.List;

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
  
  public QueryRunner initializeWith(ConsumerWithException<Connection> initializer) {
    return new QueryRunner(connection.andThen(initializer), finalizer);
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
 public <T> T select(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception {
    return run(c -> {
      try (PreparedStatement stmt = c.prepareStatement(sql);) {
        fillStatement(stmt, Arrays.asList(params));
        
        try (ResultSet rs = stmt.executeQuery();) {
          return resultSetHandler.handle(rs);
        }
      }
    });
  }

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
  public int execute(String sql, Object... params) throws Exception {
    return run(c -> {
      try (PreparedStatement statement = connection.get().prepareStatement(sql);) {
        fillStatement(statement, Arrays.asList(params));

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
    try (PreparedStatement stmt = connection.get().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
      fillStatement(stmt, Arrays.asList(params));
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
      try (PreparedStatement statement = connection.get().prepareStatement(sql);) {
        for (int i = 0; i < params.size(); i++) {
          this.fillStatement(statement, params.get(i));
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
      try (PreparedStatement stmt = connection.get().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
        for (List<Object> params : batchParams) {
          this.fillStatement(stmt, params);
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
   * {@link Connection#setAutoCommit(boolean)} is set to false before any calls are made and set to true afterwards.</p>
   * 
   * <p>Make sure to call {@link Connection#commit()} or {@link Connection#rollback()}!</p>
   * 
   * @param txQueryRunner
   *    Make sure to use this {@link QueryRunner} in the transaction block
   * @throws Exception
   *    if anything goes wrong
   */
  public void tx(BiConsumerWithException<QueryRunner, QueryRunner.Transaction> txQueryRunner) throws Exception {
    Connection _connection = connection.get();
    _connection.setAutoCommit(false);
    QueryRunner queryRunner = new QueryRunner(() -> _connection, c -> {});
    try {
      txQueryRunner.accept(queryRunner, new QueryRunner.Transaction(_connection));
    } finally {
      finalizer.accept(_connection);
      _connection.setAutoCommit(true);
    }
  }

  QueryRunner(SupplierWithException<Connection> connection, ConsumerWithException<Connection> finalizer) {
    this.connection = connection;
    this.finalizer = finalizer;
  }

  /*
   * Fill the <code>PreparedStatement</code> replacement parameters with the
   * given objects.
   * 
   * @param stmt PreparedStatement to fill
   * 
   * @param params Query replacement parameters; <code>null</code> is a valid
   * value to pass in.
   * 
   * @throws SQLException if a database access error occurs
   */
  private void fillStatement(PreparedStatement statement, List<Object> params) throws SQLException {
    for (int i = 0; i < params.size(); i++) {
      Object param = params.get(i);
      if (param != null) {
        statement.setObject(i + 1, param);
      } else {
        // VARCHAR works with many drivers regardless
        // of the actual column type. Oddly, NULL and
        // OTHER don't work with Oracle's drivers.
        int sqlType = Types.VARCHAR;
        sqlType = statement.getParameterMetaData().getParameterType(i + 1);
        statement.setNull(i + 1, sqlType);
      }
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
