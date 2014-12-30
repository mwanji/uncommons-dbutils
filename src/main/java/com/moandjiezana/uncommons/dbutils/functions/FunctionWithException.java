package com.moandjiezana.uncommons.dbutils.functions;

@FunctionalInterface
public interface FunctionWithException<T, R> {

  R apply(T t) throws Exception;
}
