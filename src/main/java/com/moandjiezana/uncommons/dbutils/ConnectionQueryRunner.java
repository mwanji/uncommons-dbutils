/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

class ConnectionQueryRunner implements QueryRunner {

  private final Connection connection;

  ConnectionQueryRunner(Connection connection) {
    this.connection = connection;
  }

  @Override
  public <T> T select(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception {
    try (PreparedStatement stmt = connection.prepareStatement(sql);) {
      fillStatement(stmt, Arrays.asList(params));
      
      try (ResultSet rs = stmt.executeQuery();) {
        return resultSetHandler.handle(rs);
      }
    }
  }

  @Override
  public int execute(String sql, Object... params) throws Exception {
    try (PreparedStatement statement = connection.prepareStatement(sql);) {
      fillStatement(statement, Arrays.asList(params));

      return statement.executeUpdate();
    }
  }
  @Override
  public <T> T insert(String sql, ResultSetHandler<T> resultSetHandler, Object... params) throws Exception {
    try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
      fillStatement(stmt, Arrays.asList(params));
      stmt.executeUpdate();
      
      try (ResultSet resultSet = stmt.getGeneratedKeys();) {
        return resultSetHandler.handle(resultSet);
      }
    }
  }

  @Override
  public int[] batch(String sql, List<List<Object>> params) throws Exception {
    try (PreparedStatement statement = connection.prepareStatement(sql);) {
      for (int i = 0; i < params.size(); i++) {
        this.fillStatement(statement, params.get(i));
        statement.addBatch();
      }

      return statement.executeBatch();
    }
  }

  @Override
  public <T> T batch(String sql, ResultSetHandler<T> resultSetHandler, List<List<Object>> batchParams) throws Exception {
    try (PreparedStatement stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {
      for (List<Object> params : batchParams) {
        this.fillStatement(stmt, params);
        stmt.addBatch();
      }
      stmt.executeBatch();
      ResultSet rs = stmt.getGeneratedKeys();

      return resultSetHandler.handle(rs);
    }
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
}
