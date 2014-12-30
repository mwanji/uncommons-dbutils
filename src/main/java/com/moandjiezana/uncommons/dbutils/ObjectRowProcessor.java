package com.moandjiezana.uncommons.dbutils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.function.Predicate;

import com.moandjiezana.uncommons.dbutils.functions.BiFunctionWithException;

public class ObjectRowProcessor<T> implements RowProcessor<T> {
  
  public static Predicate<String> table(String tableName) {
    return t -> t.equalsIgnoreCase(tableName);
  }
  
  public static final <U> BiFunctionWithException<Class<U>, String, Field> matching() {
    return (cl, col) -> cl.getDeclaredField(col);
  }
  
  public static final <U> BiFunctionWithException<Class<U>, String, Field> underscoresToCamel() {
    return (cl, col) -> {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < col.length(); i++) {
        char ch = col.charAt(i);
        if (ch == '_') {
          i++;
          sb.append(Character.toUpperCase(col.charAt(i)));
        } else {
          sb.append(Character.toLowerCase(ch));
        }
      }
      
      return cl.getDeclaredField(sb.toString());
    };
  }
  
  private final Class<T> objectClass;
  private final BiFunctionWithException<Class<T>, String, Field> columnToFieldMapper;
  private final Converters converters = Converters.INSTANCE;
  private final Predicate<String> tablePredicate;
  
  public ObjectRowProcessor(Class<T> objectClass, BiFunctionWithException<Class<T>, String, Field> columnToFieldMapper) {
    this(objectClass, t -> true, columnToFieldMapper);
  }

  public ObjectRowProcessor(Class<T> objectClass, Predicate<String> tablePredicate, BiFunctionWithException<Class<T>, String, Field> columnToFieldMapper) {
    this.objectClass = objectClass;
    this.tablePredicate = tablePredicate;
    this.columnToFieldMapper = columnToFieldMapper;
  }

  @Override
  public T handle(ResultSet rs) throws Exception {
    Constructor<T> constructor = objectClass.getDeclaredConstructor();
    constructor.setAccessible(true);
    T instance = constructor.newInstance();
      for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
        String jdbcTableName = rs.getMetaData().getTableName(i).toLowerCase();
        if (tablePredicate.test(jdbcTableName)) {
          String columnLabel = rs.getMetaData().getColumnLabel(i).toLowerCase();
          try {
            Field field = columnToFieldMapper.apply(objectClass, columnLabel);
            if (field == null) {
              continue;
            }
            field.setAccessible(true);
            if (field.getType() == boolean.class) {
              field.set(instance, rs.getBoolean(i));
            } else if (field.getType() == int.class) {
              field.set(instance, rs.getInt(i));
            } else {
              field.set(instance, converters.convert(field.getType(), rs.getObject(i)));
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        
    };
    
    return instance;
  }
}
