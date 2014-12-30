package com.moandjiezana.uncommons.dbutils.functions;

@FunctionalInterface
public interface BiFunctionWithException<T, U, R> {

  R apply(T t, U u) throws Exception;
}
