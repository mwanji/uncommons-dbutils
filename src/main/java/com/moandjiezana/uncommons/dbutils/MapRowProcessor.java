package com.moandjiezana.uncommons.dbutils;

import java.sql.ResultSet;
import java.util.Map;

import com.moandjiezana.uncommons.dbutils.functions.PredicateWithException;

/**
 * Converts a {@link ResultSet} row to a case-insensitive {@link Map}, which does not support null keys.
 */
public class MapRowProcessor implements RowProcessor<Map<String, Object>> {
  
  /**
   * Restricts the 
   * @param tableName
   * @return
   */
  public static PredicateWithException<ResultSet, Integer> table(String tableName) {
    return (rs, i) -> rs.getMetaData().getTableName(i).equalsIgnoreCase(tableName);
  }

  private final PredicateWithException<ResultSet, Integer> columnPredicate;
  
  public MapRowProcessor() {
    this((rs, i) -> true);
  }
  
  public MapRowProcessor(PredicateWithException<ResultSet, Integer> columnPredicate) {
    this.columnPredicate = columnPredicate;
  }

  @Override
  public Map<String, Object> handle(ResultSet resultSet) throws Exception {
    Map<String, Object> map = new CaseInsensitiveMap<Object>();
    
    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
      if (!columnPredicate.test(resultSet, i)) {
        continue;
      }
      map.put(resultSet.getMetaData().getColumnLabel(i), resultSet.getObject(i));
    }
    
    return map;
  }
}
