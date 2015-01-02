package com.moandjiezana.uncommons.dbutils;

import static com.moandjiezana.uncommons.dbutils.ObjectRowProcessor.beanInstanceCreator;
import static com.moandjiezana.uncommons.dbutils.ObjectRowProcessor.properties;

import java.sql.ResultSet;
import java.util.Optional;

import com.moandjiezana.uncommons.dbutils.functions.BiConsumerWithException;

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
  
  
  default RowProcessor<T> combine(BiConsumerWithException<T, Object> strategy, RowProcessor<?>... rowProcessors) {
    return (rs) -> {
      T result = handle(rs);
      for (RowProcessor<?> rowProcessor : rowProcessors) {
        strategy.accept(result, rowProcessor.handle(rs));
      }
      
      return result;
    };
  }

  /**
   * @param <T>
   *    the type to convert the column to
   * @return
   *    {@link RowProcessor} that gets the first column of the row
   * @see ColumnRowProcessor
   */
  static <T> RowProcessor<T> firstColumn() {
    return firstColumn(null);
  }

  /**
   * @param objectClass
   *    Specify the class when a conversion is necessary
   * @param <T>
   *    the type to convert the column to
   * @return
   *    {@link RowProcessor} that gets the first column of the row
   * @see ColumnRowProcessor
   */
  static <T> RowProcessor<T> firstColumn(Class<T> objectClass) {
    return new ColumnRowProcessor<T>(1, objectClass);
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


  static <T> RowProcessor<T> fieldsProcessor(Class<T> objectClass) {
    return new ObjectRowProcessor<T>(ObjectRowProcessor.noArgsCreator(objectClass), ObjectRowProcessor.matching());
  }


  static <T> RowProcessor<T> beanProcessor(Class<T> beanClass) {
    return new ObjectRowProcessor<T>(beanInstanceCreator(beanClass), properties(beanClass));
  }
}
