package com.moandjiezana.uncommons.dbutils;

import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import com.moandjiezana.uncommons.dbutils.functions.BiConsumerWithException;

/**
 * Converts a {@link ResultSet} to a {@link Map}. A key is extracted from each row. If that key is already in the {@link Map}, its value is passed on to the combiner function.
 * Otherwise, a new key/value pair is added and the new value passed to the combiner.
 * 
 * @param <K>
 *    The type of the keys
 * @param <V>
 *    The type of the values
 */
public class MapResultSetHandler<K,  V> implements ResultSetHandler<Map<K,  V>> {
  
  private final RowProcessor<K> keyExtractor;
  private final RowProcessor<V> rowProcessor;
  private BiConsumerWithException<V, ResultSet> combiner;

  /**
   * @param keyExtractor
   *    Creates a {@link Map} key
   * @param rowProcessor
   *    Creates a {@link Map} value for each row
   * @param combiner
   *    Can be null.
   */
  public MapResultSetHandler(RowProcessor<K> keyExtractor, RowProcessor<V> rowProcessor, BiConsumerWithException<V, ResultSet> combiner) {
    this.keyExtractor = keyExtractor;
    this.rowProcessor = rowProcessor;
    this.combiner = combiner != null ? combiner : (value, rs) -> {};
  }

  /**
   * @return an empty {@link Map} if the {@link ResultSet} is empty
   */
  @Override
  public Map<K,  V> handle(ResultSet rs) throws Exception {
    LinkedHashMap<K,  V> map = new LinkedHashMap<>();
    Function<K, V> valueMapper = key -> {
      try {
        return rowProcessor.handle(rs);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    
    while (rs.next()) {
      combiner.accept(map.computeIfAbsent(keyExtractor.handle(rs), valueMapper), rs);
    }
    
    return map;
  }
}
