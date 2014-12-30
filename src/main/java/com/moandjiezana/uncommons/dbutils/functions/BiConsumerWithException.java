package com.moandjiezana.uncommons.dbutils.functions;

@FunctionalInterface
public interface BiConsumerWithException<T, U> {

  void accept(T t, U u) throws Exception;
}
