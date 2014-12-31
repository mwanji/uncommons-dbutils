package com.moandjiezana.uncommons.dbutils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.moandjiezana.uncommons.dbutils.functions.BiConsumerWithException;
import com.moandjiezana.uncommons.dbutils.functions.SupplierWithException;

/**
 * Provides an asynchronous wrapper around {@link QueryRunner}. Instances are obtained from an existing {@link QueryRunner}, so that all configuration is conserved.
 * 
 * @see QueryRunner#toAsync()
 * @see QueryRunner#toAsync(Executor)
 */
public class AsyncQueryRunner {

  private final Executor executorService;
  private final QueryRunner queryRunner;
  
  AsyncQueryRunner(QueryRunner queryRunner, Executor executor) {
    this.queryRunner = queryRunner;
    this.executorService = executor;
  }
  
  public <T> CompletableFuture<T> select(String sql, ResultSetHandler<T> resultSetHandler, Object... params) {
    return run(() -> queryRunner.select(sql, resultSetHandler, params));
  }
  
  public CompletableFuture<Integer> execute(String sql, Object... params) {
    return run(() -> queryRunner.execute(sql, params));
  }
  
  public <T> CompletableFuture<T> insert(String sql, ResultSetHandler<T> resultSetHandler, Object... params) {
    return run(() -> queryRunner.insert(sql, resultSetHandler, params));
  }
  
  public CompletableFuture<int[]> batch(String sql, List<List<Object>> params) {
    return run(() -> queryRunner.batch(sql, params));
  }
  
  public <T> CompletableFuture<T> batchInsert(String sql, ResultSetHandler<T> resultSetHandler, List<List<Object>> batchParams) {
    return run(() -> queryRunner.batchInsert(sql, resultSetHandler, batchParams));
  }
  
  /**
   * @param txQueryRunner
   *    Make sure to use this {@link QueryRunner} in the transaction block
   * @return a {@link CompletableFuture} that completes when the transaction is finished
   */
  public CompletableFuture<Void> tx(BiConsumerWithException<QueryRunner, QueryRunner.Transaction> txQueryRunner) {
    return run(() -> { 
      try {
        queryRunner.tx(txQueryRunner);
        return null;
      } catch (Exception e) {
        throw propagate(e);
      }
    });
  }
  
  private <T> CompletableFuture<T> run(SupplierWithException<T> s) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return s.get();
      } catch (Exception e) {
        throw propagate(e);
      }
    }, executorService);
  }
  
  private static RuntimeException propagate(Exception e) {
    if (e instanceof RuntimeException) {
      return (RuntimeException) e;
    }
    
    return new RuntimeException(e);
  }
}
