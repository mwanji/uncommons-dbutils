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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Converts {@link ResultSet}s to other objects.
 *
 * @param <T>
 *    the target type the input {@link ResultSet} will be converted to.
 */
@FunctionalInterface
public interface ResultSetHandler<T> {

  /**
   * Turn the {@link ResultSet} into an instance of T.
   *
   * @param rs
   *    The {@link ResultSet} to handle. It has not been touched before being passed to this method.
   *    
   * @return An Object initialized with {@link ResultSet} data. Implementations may return <code>null</code>.
   *
   * @throws Exception
   *    if a database access error occurs
   */
  T handle(ResultSet rs) throws Exception;

  /**
   * @param rowProcessor
   *    creates an instance of T
   * @param <T>
   *    the instance type
   * @return an instance of T from the first row of the {@link ResultSet} or null, if the {@link ResultSet} is empty
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
   * @param resultSetHandler
   *    processes the {@link ResultSet}
   * @param <T>
   *    the type of the instance wrapped in an {@link Optional}
   * @return the result of invoking the underlying {@link ResultSetHandler}, wrapped in an {@link Optional}.
   */
  static <T> ResultSetHandler<Optional<T>> optional(ResultSetHandler<T> resultSetHandler) {
    return rs -> {
      return Optional.ofNullable(resultSetHandler.handle(rs));
    };
  }

  /**
   * 
   * @param rowProcessor
   *    creates an instance of T
   * @param <T>
   *    the instance type
   * @return a {@link List} of T or an empty {@link List} if the {@link ResultSet} is empty.
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
   * @param keyColumn
   *    the name of the column the map key can be extracted from
   * @param keyClass
   *    the type of the {@link Map} keys
   * @param rowProcessor
   *    creates an instance of V
   * @param <K>
   *    the type of the {@link Map} keys
   * @param <V>
   *    the type of the {@link Map} values
   * 
   * @return a {@link Map} with keys taken from keyColumn or an empty {@link Map} if the {@link ResultSet} is empty.
   * 
   * @see MapResultSetHandler
   */
  static <K, V> ResultSetHandler<Map<K, V>> map(String keyColumn, Class<K> keyClass, RowProcessor<V> rowProcessor) {
    return new MapResultSetHandler<>(column("id", keyClass), rowProcessor);
  }

  /**
   * Returns nothing.
   */
  static ResultSetHandler<Void> VOID = rs -> { return null; };
}