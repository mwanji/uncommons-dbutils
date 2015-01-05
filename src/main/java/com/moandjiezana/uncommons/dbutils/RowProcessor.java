package com.moandjiezana.uncommons.dbutils;

import static com.moandjiezana.uncommons.dbutils.ObjectRowProcessor.beanInstanceCreator;
import static com.moandjiezana.uncommons.dbutils.ObjectRowProcessor.fields;
import static com.moandjiezana.uncommons.dbutils.ObjectRowProcessor.matching;
import static com.moandjiezana.uncommons.dbutils.ObjectRowProcessor.noArgsCreator;
import static com.moandjiezana.uncommons.dbutils.ObjectRowProcessor.properties;

import java.sql.ResultSet;
import java.util.Optional;
import java.util.function.Supplier;

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
  
  /**
   * @param withRowProcessor
   * @param combiner
   * @return a new RowProcessor instance that returns the result of this {@link #handle(ResultSet)}, after 
   */
  default <U> RowProcessor<T> with(RowProcessor<U> withRowProcessor, BiConsumerWithException<T, U> combiner) {
    return (rs) -> {
      T result = handle(rs);
      
      combiner.accept(result, withRowProcessor.handle(rs));
      
      return result;
    };
  }
  
  /**
   * @param supplier
   *    creates the per-row container
   * @return a RowProcessor that is useful for chaining {@link #with(RowProcessor, BiConsumerWithException)}.
   */
  static <T> RowProcessor<T> container(Supplier<T> supplier) {
    return rs -> supplier.get();
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
    return ColumnRowProcessor.column(1, objectClass);
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

  /**
   * @param objectClass
   *    The class to map the rows to
   * @param <T>
   *    The type of the mapped row
   * @return A RowProcessor that uses field access to populate instances with values from columns with the same name
   */
  static <T> RowProcessor<T> fieldsProcessor(Class<T> objectClass) {
    return new ObjectRowProcessor<T>(noArgsCreator(objectClass), matching(fields(objectClass)));
  }

  /**
   * @param beanClass
   *    The class to map the rows to
   * @param <T>
   *    The type of the mapped row
   * @return A Row Processor that uses JavaBean conventions to create and populate instances of T
   */
  static <T> RowProcessor<T> beanProcessor(Class<T> beanClass) {
    return new ObjectRowProcessor<T>(beanInstanceCreator(beanClass), matching(properties(beanClass)));
  }
}
