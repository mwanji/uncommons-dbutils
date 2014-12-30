package com.moandjiezana.uncommons.dbutils;

import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

import com.moandjiezana.uncommons.dbutils.functions.FunctionWithException;

/**
 * Converts a {@link ResultSet} to a {@link Map} with one entry per row.
 * 
 * @param <K>
 *    The type of the keys
 * @param <V>
 *    The type of the values
 */
public class MapResultSetHandler<K,  V> implements ResultSetHandler<Map<K,  V>> {
  
  /**
   * Extracts the key from a single column.
   * 
   * @param column
   *    the name of the column to extract the key from
   * @param columnClass
   *    the type of the column
   * @param <U>
   *    the type of the instance
   * @return an instance of U
   */
  public static <U> FunctionWithException<ResultSet, U> column(String column, Class<U> columnClass) {
    return rs -> Converters.INSTANCE.convert(columnClass, rs.getObject(column));
  }
  
  private final FunctionWithException<ResultSet, K> keyExtractor;
  private final RowProcessor<V> rowProcessor;

  /**
   * @param keyExtractor
   *    Creates a {@link Map} key for each row
   * @param rowProcessor
   *    Creates a {@link Map} value for each row
   */
  public MapResultSetHandler(FunctionWithException<ResultSet, K> keyExtractor, RowProcessor<V> rowProcessor) {
    this.keyExtractor = keyExtractor;
    this.rowProcessor = rowProcessor;
  }

  /**
   * @return an empty {@link Map} if the {@link ResultSet} is empty
   */
  @Override
  public Map<K,  V> handle(ResultSet rs) throws Exception {
    LinkedHashMap<K,  V> map = new LinkedHashMap<>();
    
    while (rs.next()) {
      map.put(keyExtractor.apply(rs), rowProcessor.handle(rs));
    }
    
    return map;
  }
}
