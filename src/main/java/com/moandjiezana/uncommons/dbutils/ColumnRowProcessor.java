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

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <code>RowProcessor</code> implementation that converts one
 * <code>ResultSet</code> column into an Object. This class is thread safe.
 *
 * @param <T>
 *          The type of the scalar
 */
public class ColumnRowProcessor<T> implements RowProcessor<T> {
  
  private final int columnIndex;
  private final String columnName;

  /**
   * Creates a new instance of ScalarHandler.
   *
   * @param columnIndex
   *          The index of the column to retrieve from the
   *          <code>ResultSet</code>.
   */
  public ColumnRowProcessor(int columnIndex) {
    this(columnIndex, null);
  }

  /**
   * Creates a new instance of ScalarHandler.
   *
   * @param columnName
   *          The name of the column to retrieve from the <code>ResultSet</code>
   */
  public ColumnRowProcessor(String columnName) {
    this(1, columnName);
  }

  private ColumnRowProcessor(int columnIndex, String columnName) {
    this.columnIndex = columnIndex;
    this.columnName = columnName;
  }

  /**
   * Returns one <code>ResultSet</code> column as an object via the
   * <code>ResultSet.getObject()</code> method that performs type conversions.
   * 
   * @param rs
   *          <code>ResultSet</code> to process.
   * @return The column or <code>null</code> if there are no rows in the
   *         <code>ResultSet</code>.
   *
   * @throws SQLException
   *           if a database access error occurs
   * @throws ClassCastException
   *           if the class datatype does not match the column type
   */
  // We assume that the user has picked the correct type to match the column
  // so getObject will return the appropriate type and the cast will succeed.
  @SuppressWarnings("unchecked")
  @Override
  public T handle(ResultSet rs) throws SQLException {
    if (this.columnName == null) {
      return (T) rs.getObject(this.columnIndex);
    }
    return (T) rs.getObject(this.columnName);
  }
}
