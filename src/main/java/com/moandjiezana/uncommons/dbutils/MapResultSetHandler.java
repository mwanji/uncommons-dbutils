package com.moandjiezana.uncommons.dbutils;

import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

import com.moandjiezana.uncommons.dbutils.functions.FunctionWithException;

public class MapResultSetHandler<K, T> implements ResultSetHandler<Map<K, T>> {
  
  public static <U> FunctionWithException<ResultSet, U> column(String column, Class<U> columnClass) {
    return rs -> Converters.INSTANCE.convert(columnClass, rs.getObject(column));
  }
  
  private final FunctionWithException<ResultSet, K> keyExtractor;
  private final RowProcessor<T> rowProcessor;

  public MapResultSetHandler(FunctionWithException<ResultSet, K> keyExtractor, RowProcessor<T> rowProcessor) {
    this.keyExtractor = keyExtractor;
    this.rowProcessor = rowProcessor;
  }

  @Override
  public Map<K, T> handle(ResultSet rs) throws Exception {
    LinkedHashMap<K, T> map = new LinkedHashMap<>();
    
    while (rs.next()) {
      map.put(keyExtractor.apply(rs), rowProcessor.handle(rs));
    }
    
    return map;
  }
}
