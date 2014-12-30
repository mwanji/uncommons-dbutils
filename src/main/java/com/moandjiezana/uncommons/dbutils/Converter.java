package com.moandjiezana.uncommons.dbutils;

/**
 * @param <T>
 *    The type to convert values to
 */
@FunctionalInterface
public interface Converter<T> {
  
  /**
   * 
   * @param targetClass
   *    The class to convert value to
   * @param value
   *    The value to be converted
   * @return an instance of T
   */
  T convert(Class<T> targetClass, Object value);
}
