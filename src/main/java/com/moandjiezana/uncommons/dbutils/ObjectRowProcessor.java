package com.moandjiezana.uncommons.dbutils;

import static java.util.stream.Collectors.toMap;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.moandjiezana.uncommons.dbutils.functions.FunctionWithException;

public class ObjectRowProcessor<T> implements RowProcessor<T> {
  
  /**
   * @param metaDataMapper
   *    a mapper that returns the field or method corresponding to a given name
   * @param <T>
   *    the type the returned {@link MetaDataMapper} accepts
   * @return a mapper that does not change the column name
   */
  public static <T> MetaDataMapper<T, Optional<AccessibleObject>> matching(MetaDataMapper<String, Optional<AccessibleObject>> metaDataMapper) {
    return (rs, i, instance) -> {
      String columnLabel = rs.getMetaData().getColumnLabel(i);
      
      return metaDataMapper.apply(rs, i, columnLabel);
    };
  }

  /**
   * @param metaDataMapper a mapper that returns the field or method corresponding to a given name
   * @param <T>
   *    the type the returned {@link MetaDataMapper} accepts
   * @return a mapper that converts names with underscores to camelCase
   */
  public static <T> MetaDataMapper<T, Optional<AccessibleObject>> underscoresToCamel(MetaDataMapper<String, Optional<AccessibleObject>> metaDataMapper) {
    Map<String, String> cache = new HashMap<>();
    
    return (rs, columnIndex, instance) -> {
      String mappedColumn = cache.computeIfAbsent(rs.getMetaData().getColumnLabel(columnIndex), col -> {
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

        return sb.toString();
      });
      
      return metaDataMapper.apply(rs, columnIndex, mappedColumn);
    };
  }

  /**
   * @param beanClass the class to be converted to
   * @return a mapper that matches the given names to JavaBean-style properties
   */
  public static MetaDataMapper<String, Optional<AccessibleObject>> properties(Class<?> beanClass) {
    try {
      BeanInfo beanInfo = Introspector.getBeanInfo(beanClass);
      PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
      
      Map<String, Optional<AccessibleObject>> columnSetters = Arrays.stream(propertyDescriptors).collect(toMap(pd -> pd.getName().toLowerCase(), pd -> Optional.ofNullable(pd.getWriteMethod())));
      
      return (rs, i, columnName) -> columnSetters.getOrDefault(columnName.toLowerCase(), Optional.empty());
    } catch (IntrospectionException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * @param objectClass the class to be converted to
   * @return a mapper that matches the given names to fields
   */
  public static MetaDataMapper<String, Optional<AccessibleObject>> fields(Class<?> objectClass) {
    Map<String, Optional<AccessibleObject>> fields = Arrays.stream(objectClass.getDeclaredFields()).collect(toMap(field -> field.getName().toLowerCase(), field -> Optional.of(field)));
    
    return (rs, i, columnName) -> fields.getOrDefault(columnName.toLowerCase(), Optional.empty());
  }
  
  public static <T> MetaDataMapper<T, Optional<AccessibleObject>> table(String table, MetaDataMapper<T, Optional<AccessibleObject>> metaDataMapper) {
    return (rs, i, o) -> {
      String tableName = rs.getMetaData().getTableName(i);
      if (!tableName.equalsIgnoreCase(table)) {
        return Optional.empty();
      }
      
      return metaDataMapper.apply(rs, i, o);
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
  public static interface MetaDataMapper<T, R> {
    public R apply(ResultSet rs, int columnIndex, T instance) throws Exception;
  }
  
  private final FunctionWithException<ResultSet, T> instanceCreator;
  private final MetaDataMapper<T, Optional<AccessibleObject>> metaDataMapper;
  private final Converters converters = Converters.INSTANCE;
  
  public ObjectRowProcessor(FunctionWithException<ResultSet, T> instanceCreator, MetaDataMapper<T, Optional<AccessibleObject>> metaDataMapper) {
    this.instanceCreator = instanceCreator;
    this.metaDataMapper = metaDataMapper;
  }

  @Override
  public T handle(ResultSet rs) throws Exception {
    T instance = instanceCreator.apply(rs);
    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
      Optional<AccessibleObject> optional = metaDataMapper.apply(rs, i, instance);
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
