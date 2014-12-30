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
 * Converts the specified {@link ResultSet} column into an instance of T. This class is thread safe.
 * 
 * @param <T>
 *    The type of the column. When no class is specified via the constructor, this must match or be a super-type of what is returned by the {@link ResultSet}.
 */
public class ColumnRowProcessor<T> implements RowProcessor<T> {
  
  private final int columnIndex;
  private final String columnName;
  private final Class<T> objectClass;

  /**
   * 
   * @param columnIndex
   *    The index of the column to retrieve from the {@link ResultSet}.
   */
  public ColumnRowProcessor(int columnIndex) {
    this(columnIndex, null);
  }

  /**
   * @param columnIndex
   *    The index of the column to retrieve from the {@link ResultSet}.
   * @param objectClass
   *    The class of the returned instance. Makes custom conversion possible.
   */
  public ColumnRowProcessor(int columnIndex, Class<T> objectClass) {
    this(columnIndex, null, objectClass);
  }
  
  /**
   * @param columnName
   *    The name of the column to retrieve from the {@link ResultSet}.
   */
  public ColumnRowProcessor(String columnName) {
    this(columnName, null);
  }

  /**
   * @param columnName
   *    The name of the column to retrieve from the {@link ResultSet}.
   * @param objectClass
   *    The class of the returned instance. Makes custom conversion possible.
   */
  public ColumnRowProcessor(String columnName, Class<T> objectClass) {
    this(0, columnName, objectClass);
  }

  /**
   * @param rs
   *    {@link ResultSet} to process.
   * @return an instance of T, either by conversion if a class was specified or by casting.
   * @throws SQLException
   *    if a database access error occurs
   * @throws ClassCastException
   *    if no class was specified and T does not match the column type
   */
  @SuppressWarnings("unchecked")
  @Override
  public T handle(ResultSet rs) throws SQLException {
    Object raw = this.columnIndex == 0 ? rs.getObject(columnName) : rs.getObject(columnIndex);
    
    if (objectClass == null) {
      return (T) raw;
    }
    
    return Converters.INSTANCE.convert(objectClass, raw);
  }

  private ColumnRowProcessor(int columnIndex, String columnName, Class<T> objectClass) {
    this.columnIndex = columnIndex;
    this.columnName = columnName;
    this.objectClass = objectClass;
  }
}
