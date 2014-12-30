package com.moandjiezana.uncommons.dbutils;

@FunctionalInterface
public interface Converter<T> {
  
  T convert(Class<T> targetClass, Object value);
}
