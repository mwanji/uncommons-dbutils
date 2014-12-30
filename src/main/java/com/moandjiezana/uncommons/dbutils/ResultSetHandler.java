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

import static com.moandjiezana.uncommons.dbutils.MapResultSetHandler.column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implementations of this interface convert {@link ResultSet}s into other objects.
 *
 * @param <T>
 *          the target type the input {@link ResultSet} will be converted to.
 */
public interface ResultSetHandler<T> {

  /**
   * Turn the {@link ResultSet} into an Object.
   *
   * @param rs
   *          The {@link ResultSet} to handle. It has not been touched
   *          before being passed to this method.
   *
   * @return An Object initialized with {@link ResultSet} data. Implementations may return <code>null</code> if the
   *         {@link ResultSet} contained 0 rows.
   *
   * @throws SQLException
   *           if a database access error occurs
   */
  T handle(ResultSet rs) throws Exception;

  /**
   * Processes the first row of the {@link ResultSet}. If it is empty, returns null.
   */
  static <T> ResultSetHandler<T> single(RowProcessor<T> rowProcessor) {
    return rs -> {
      if (!rs.next()) {
        return null;
      }

      return rowProcessor.handle(rs);
    };
  }
  
  /**
   * Processes the first row of the {@link ResultSet}, wrapped in an {@link Optional}. If the {@link ResultSet} is empty, returns an empty {@link Optional}.
   */
  static <T> ResultSetHandler<Optional<T>> maybe(RowProcessor<T> rowProcessor) {
    return rs -> {
      if (!rs.next()) {
        return Optional.empty();
      }
      
      return Optional.ofNullable(rowProcessor.handle(rs));
    };
  }

  /**
   * Processes each row in the {@link ResultSet}. If it is empty, returns an empty {@link List}.
   */
  static <T> ResultSetHandler<List<T>> list(RowProcessor<T> rowProcessor) {
    return rs -> {
      List<T> results = new ArrayList<>();
      while (rs.next()) {
        results.add(rowProcessor.handle(rs));
      }

      return results;
    };
  }
  
  /**
   * Converts a {@link ResultSet} to a {@link Map} of keys taken from keyColumn and values returned by rowProcessor.
   * 
   * For more customisation, see {@link MapResultSetHandler}.
   */
  static <K, T> ResultSetHandler<Map<K, T>> map(String keyColumn, Class<K> keyClass, RowProcessor<T> rowProcessor) {
    return new MapResultSetHandler<>(column("id", keyClass), rowProcessor);
  }

  static ResultSetHandler<Void> VOID = rs -> { return null; };
}