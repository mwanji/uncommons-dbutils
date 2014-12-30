package com.moandjiezana.uncommons.dbutils.functions;

@FunctionalInterface
public interface PredicateWithException<T, U> {

  boolean test(T t, U u) throws Exception;
}
