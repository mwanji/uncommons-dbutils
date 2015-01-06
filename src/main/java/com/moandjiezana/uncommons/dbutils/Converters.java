package com.moandjiezana.uncommons.dbutils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Groups {@link Converter}s together.
 * 
 * {@link Converters#INSTANCE} is a singleton that provides defaults and makes registered {@link Converter}s available globally
 *
 */
public interface Converters {
  
  /**
   * @param targetClass
   *    the class to convert value to
   * @param value
   *    the object to be converted
   * @param <T>
   *    the type of the converted value
   * @return an instance of T
   */
  <T> T convert(Class<T> targetClass, Object value);
  
  /**
   * @param targetClass
   *    the type handled by this {@link Converter}
   * @param converter
   *    the {@link Converter}
   * @param <T>
   *    the type of the converted value
   */
  <T> void register(Class<T> targetClass, Converter<T> converter);
  
  /**
   * A singleton that provides defaults and makes registered {@link Converter}s available globally.
   * 
   * Supports all types returned by {@link ResultSet}#getXxx() methods, {@link Instant} and any class with a static <code>valueOf(String)</code> method.
   * 
   * If no conversion can be performed, returns <code>null</code>.
   */
  static final Converters INSTANCE = new Converters() {
    private final Map<Class<?>, Converter<?>> converters = new HashMap<>();
    
    {
      register(Instant.class, (cl, value) -> ((Timestamp) value).toInstant());
    }

    @Override
    public <T> T convert(Class<T> targetClass, Object value) {
      if (value == null) {
        return null;
      }
      
      if (targetClass.isAssignableFrom(value.getClass())) {
        return targetClass.cast(value);
      }
      
      if (converters.containsKey(targetClass)) {
        @SuppressWarnings("unchecked")
        Converter<T> converter = (Converter<T>) converters.get(targetClass);
        
        return converter.convert(targetClass, value);
      }
      
      Optional<Method> valueOfMethod = Arrays.stream(targetClass.getMethods())
        .filter(m -> Modifier.isStatic(m.getModifiers()))
        .filter(m -> m.getName().equals("valueOf"))
        .filter(m -> m.getParameterCount() == 1 && m.getParameterTypes()[0] == value.getClass())
        .findFirst();
      
      if (valueOfMethod.isPresent()) {
        try {
          return targetClass.cast(valueOfMethod.get().invoke(null, value));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }
      
      throw new IllegalArgumentException("Cannot convert to " + targetClass.getName() + " from " + value.getClass());
    }
    
    @Override
    public <T> void register(Class<T> targetClass, Converter<T> converter) {
      converters.put(targetClass, converter);
    }
  };
}
