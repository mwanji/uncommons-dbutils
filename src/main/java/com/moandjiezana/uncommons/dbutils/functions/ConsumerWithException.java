package com.moandjiezana.uncommons.dbutils.functions;

@FunctionalInterface
public interface ConsumerWithException<T> {

  void accept(T t) throws Exception;
}
