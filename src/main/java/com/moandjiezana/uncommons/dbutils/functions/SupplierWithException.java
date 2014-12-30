package com.moandjiezana.uncommons.dbutils.functions;

import java.util.Objects;

public interface SupplierWithException<T> {

  T get() throws Exception;
  
  default SupplierWithException<T> andThen(ConsumerWithException<T> after) {
    Objects.requireNonNull(after);
    return () -> {
      T supplied = get();
      after.accept(supplied);
      
      return supplied;
    };
  }
}
