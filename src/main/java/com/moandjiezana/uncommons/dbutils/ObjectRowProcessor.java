package com.moandjiezana.uncommons.dbutils;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Optional;

import com.moandjiezana.uncommons.dbutils.functions.SupplierWithException;

public class ObjectRowProcessor<T> implements RowProcessor<T> {
  
  public static final <U> ColumnMapper<U> matching() {
    return (rs, i, instance) -> {
      String columnLabel = rs.getMetaData().getColumnLabel(i);
      for (Field field : instance.getClass().getDeclaredFields()) {
        if (field.getName().equalsIgnoreCase(columnLabel)) {
          return Optional.of(field);
        }
      }
      
      return Optional.empty();
    };
  }
  
  public static final <U> ColumnMapper<U> underscoresToCamel() {
    return (rs, columnIndex, instance) -> {
      StringBuilder sb = new StringBuilder();
      String col = rs.getMetaData().getColumnLabel(columnIndex);
      for (int i = 0; i < col.length(); i++) {
        char ch = col.charAt(i);
        if (ch == '_') {
          i++;
          sb.append(Character.toUpperCase(col.charAt(i)));
        } else {
          sb.append(Character.toLowerCase(ch));
        }
      }
      
      col = sb.toString();
      
      for (Field field : instance.getClass().getDeclaredFields()) {
        if (field.getName().equalsIgnoreCase(col)) {
          return Optional.of(field);
        }
      }
      
      return Optional.empty();
    };
  }
  
  public static final <U> SupplierWithException<U> beanCreator(Class<U> beanClass) {
    return () -> {
      return beanClass.getConstructor().newInstance();
    };
  }
  
  private final SupplierWithException<T> instanceCreator;
  private final ColumnMapper<T> columnMapper;
  private final Converters converters = Converters.INSTANCE;
  
  public ObjectRowProcessor(SupplierWithException<T> instanceCreator, ColumnMapper<T> columnMapper) {
    this.instanceCreator = instanceCreator;
    this.columnMapper = columnMapper;
  }

  @Override
  public T handle(ResultSet rs) throws Exception {
    T instance = instanceCreator.get();
    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
      Optional<AccessibleObject> optional = columnMapper.apply(rs, i, instance);
      if (!optional.isPresent()) {
        continue;
      }
      
      Accessor field = new Accessor(optional.get());
      if (field.getType() == boolean.class) {
        field.set(instance, rs.getBoolean(i));
      } else if (field.getType() == int.class) {
        field.set(instance, rs.getInt(i));
      } else {
        field.set(instance, converters.convert(field.getType(), rs.getObject(i)));
      }
    };
    
    return instance;
  }
  
  @FunctionalInterface
  private static interface ColumnMapper<T> {
    public Optional<AccessibleObject> apply(ResultSet rs, int columnIndex, T instance) throws Exception;
  }
  
  private static class Accessor {
    final Field field;
    final Method method;
    
    Accessor(AccessibleObject accessibleObject) {
      this.field = accessibleObject instanceof Field ? (Field) accessibleObject : null;
      this.method = accessibleObject instanceof Method ? (Method) accessibleObject : null;
      accessibleObject.setAccessible(true);
    }
    
    Class<?> getType() {
      return field != null ? field.getType() : method.getParameterTypes()[0];
    }
    
    void set(Object instance, Object value) {
      try {
        if (field != null) {
          field.set(instance, value);
        } else {
          method.invoke(instance, value);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
