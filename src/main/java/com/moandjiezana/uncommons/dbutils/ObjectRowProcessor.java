package com.moandjiezana.uncommons.dbutils;

import java.beans.BeanInfo;
import java.beans.ConstructorProperties;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.moandjiezana.uncommons.dbutils.functions.FunctionWithException;

public class ObjectRowProcessor<T> implements RowProcessor<T> {
  
  public static <U> ColumnMapper<U> matching() {
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
  
  public static <U> ColumnMapper<U> properties(Class<U> beanClass) {
    try {
      BeanInfo beanInfo = Introspector.getBeanInfo(beanClass);
      PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
      
      Map<String, Optional<AccessibleObject>> columnSetters = Arrays.stream(propertyDescriptors).collect(Collectors.toMap(pd -> pd.getName().toLowerCase(), pd -> Optional.ofNullable(pd.getWriteMethod())));
      
      return (rs, i, instance) -> {
        String columnLabel = rs.getMetaData().getColumnLabel(i).toLowerCase();
        return columnSetters.getOrDefault(columnLabel, Optional.empty());
      };
    } catch (IntrospectionException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static <U> ColumnMapper<U> underscoresToCamel() {
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
  
  public static <T> ColumnMapper<T> table(String table, ColumnMapper<T> columnMapper) {
    return (rs, i, o) -> {
      String tableName = rs.getMetaData().getTableName(i);
      if (!tableName.equalsIgnoreCase(table)) {
        return Optional.empty();
      }
      
      return columnMapper.apply(rs, i, o);
    };
  }
  
  public static final <U> FunctionWithException<ResultSet, U> beanInstanceCreator(Class<U> beanClass) {
    return (rs) -> {
      for (Constructor<?> c : beanClass.getConstructors()) {
        if (c.isAnnotationPresent(ConstructorProperties.class)) {
          String[] constructorProperties = c.getAnnotation(ConstructorProperties.class).value();
          Class<?>[] constructorArgumentTypes = c.getParameterTypes();
          Object[] constructorArguments = new Object[constructorProperties.length];
          
          for (int i = 0; i < constructorProperties.length; i++) {
            String constructorProperty = constructorProperties[i];
            constructorArguments[i] = rs.getObject(constructorProperty);
            Class<?> constructorArgumentType = constructorArgumentTypes[i];
            if (constructorArgumentType == boolean.class) {
              constructorArguments[i] = rs.getBoolean(constructorProperty);
            } else if (constructorArgumentType == int.class) {
              constructorArguments[i] = rs.getInt(constructorProperty);
            } else {
              constructorArguments[i] = Converters.INSTANCE.convert(constructorArgumentType, rs.getObject(constructorProperty));
            }
          }
          
          return beanClass.cast(c.newInstance(constructorArguments));
        }
      }
      
      return beanClass.getConstructor().newInstance();
    };
  }
  
  public static final <U> FunctionWithException<ResultSet, U> noArgsCreator(Class<U> objectClass) {
    return (rs) -> {
      Constructor<U> constructor = objectClass.getDeclaredConstructor();
      constructor.setAccessible(true);
      
      return constructor.newInstance();
    };
  }
  
  @FunctionalInterface
  public static interface ColumnMapper<T> {
    public Optional<AccessibleObject> apply(ResultSet rs, int columnIndex, T instance) throws Exception;
  }
  
  private final FunctionWithException<ResultSet, T> instanceCreator;
  private final ColumnMapper<T> columnMapper;
  private final Converters converters = Converters.INSTANCE;
  
  public ObjectRowProcessor(FunctionWithException<ResultSet, T> instanceCreator, ColumnMapper<T> columnMapper) {
    this.instanceCreator = instanceCreator;
    this.columnMapper = columnMapper;
  }

  @Override
  public T handle(ResultSet rs) throws Exception {
    T instance = instanceCreator.apply(rs);
    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
      Optional<AccessibleObject> optional = columnMapper.apply(rs, i, instance);
      if (!optional.isPresent()) {
        continue;
      }
      
      Accessor field = new Accessor(optional.get());
      setAccessor(instance, field, rs, i, converters);
    };
    
    return instance;
  }

  private static void setAccessor(Object instance, Accessor accessor, ResultSet rs, int i, Converters converters) throws SQLException {
    if (accessor.getType() == boolean.class) {
      accessor.set(instance, rs.getBoolean(i));
    } else if (accessor.getType() == int.class) {
      accessor.set(instance, rs.getInt(i));
    } else {
      accessor.set(instance, converters.convert(accessor.getType(), rs.getObject(i)));
    }
  }
  
  private static class Accessor {
    final Field field;
    final Method method;
    private Constructor<?> constructor;
    
    Accessor(AccessibleObject accessibleObject) {
      this.field = accessibleObject instanceof Field ? (Field) accessibleObject : null;
      this.method = accessibleObject instanceof Method ? (Method) accessibleObject : null;
      this.constructor = accessibleObject instanceof Constructor ? (Constructor<?>) accessibleObject : null;
      accessibleObject.setAccessible(true);
    }
    
    Class<?> getType() {
      return field != null ? field.getType() : method.getParameterTypes()[0];
    }
    
    void set(Object instance, Object value) {
      try {
        if (field != null) {
          field.set(instance, value);
        } else if (method != null) {
          method.invoke(instance, value);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
