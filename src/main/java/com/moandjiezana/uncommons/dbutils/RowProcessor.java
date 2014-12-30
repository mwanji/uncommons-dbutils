package com.moandjiezana.uncommons.dbutils;

import java.sql.ResultSet;
import java.util.Optional;

/**
 * Converts a {@link ResultSet} to an instance of T
 * 
 * @param <T>
 *    the type a row is converted to
 */
@FunctionalInterface
public interface RowProcessor<T> {

  /**
   * @param resultSet
   *    already pointing to the row to be processed
   * @return
   *    an instance of T
   * @throws Exception
   *    if something goes wrong
   */
  T handle(ResultSet resultSet) throws Exception;

  /**
   * @param <T>
   *    the type to convert the column to
   * @return
   *    {@link RowProcessor} that gets the first column of the row
   * @see ColumnRowProcessor
   */
  static <T> RowProcessor<T> firstColumn() {
    return new ColumnRowProcessor<T>(1);
  }

  /**
   * @param rowProcessor
   *    performs the processing
   * @param <T>
   *    the type a row is converted to
   * @return
   *    the result of processing, wrapped in an {@link Optional}
   */
  static <T> RowProcessor<Optional<T>> optional(RowProcessor<T> rowProcessor) {
    return rs -> Optional.ofNullable(rowProcessor.handle(rs));
  }
}
