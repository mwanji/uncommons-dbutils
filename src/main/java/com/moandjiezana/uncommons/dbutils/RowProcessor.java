package com.moandjiezana.uncommons.dbutils;

import java.sql.ResultSet;

@FunctionalInterface
public interface RowProcessor<T> {

  T handle(ResultSet resultSet) throws Exception;

  static <T> RowProcessor<T> firstColumn() {
    return new ColumnRowProcessor<T>(1);
  }
}
