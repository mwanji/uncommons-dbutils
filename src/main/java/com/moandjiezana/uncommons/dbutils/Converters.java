package com.moandjiezana.uncommons.dbutils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public interface Converters {
  <T> T convert(Class<T> targetClass, Object value);
  <T> void register(Class<T> targetClass, Converter<T> converter);
  
  static final Converters INSTANCE = new Converters() {
    private final Map<Class<?>, Converter<?>> converters = new HashMap<>();
    private final Converter<?> defaultConverter = (cl, value) -> {
      if (cl.isAssignableFrom(value.getClass())) {
        return cl.cast(value);
      }
      
      return null;
    };
    
    {
      register(Instant.class, (cl, value) -> ((Timestamp) value).toInstant());
    }

    @Override
    public <T> T convert(Class<T> targetClass, Object value) {
      if (value == null) {
        return null;
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
      
      @SuppressWarnings("unchecked")
      Converter<T> converter = (Converter<T>) converters.getOrDefault(targetClass, defaultConverter);
      return targetClass.cast(converter.convert(targetClass, value));
    }
    
    @Override
    public <T> void register(Class<T> targetClass, Converter<T> converter) {
      converters.put(targetClass, converter);
    }
  };
}
