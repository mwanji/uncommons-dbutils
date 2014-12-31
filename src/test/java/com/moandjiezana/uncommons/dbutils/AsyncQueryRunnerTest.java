package com.moandjiezana.uncommons.dbutils;

import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.VOID;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.list;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.moandjiezana.uncommons.dbutils.junit.TemporaryConnection;

public class AsyncQueryRunnerTest {

  @Rule
  public TemporaryConnection connection = new TemporaryConnection("jdbc:h2:mem:");
  
  @Rule
  public ExpectedException exception = ExpectedException.none();
  
  @Test
  public void should_throw_exception_via_future() throws Exception {
    AsyncQueryRunner runner = QueryRunner.create(connection.get()).toAsync();
    
    CompletableFuture<Object> future = runner.select("SELECT 1", rs -> { throw new IllegalArgumentException("test"); });
    
    exception.expectCause(isA(IllegalArgumentException.class));
    exception.expectMessage("test");
    
    future.get();
  }
  
  @Test
  public void should_use_given_executor_to_run_queries() throws Exception {
    connection.get().prepareStatement("CREATE TABLE a(id IDENTITY)").execute();
    
    AtomicInteger executorUsed = new AtomicInteger();
    QueryRunner runnerSync = QueryRunner.create(connection.get());
    AsyncQueryRunner runner = runnerSync.toAsync((r) -> { 
      r.run();
      executorUsed.incrementAndGet();
    });
    ResultSetHandler<List<Long>> handler = list(RowProcessor.firstColumn(Long.class));
    
    runner.execute("INSERT INTO a VALUES(1)");
    runner.insert("INSERT INTO a VALUES(2)", VOID);
    runner.batch("INSERT INTO a VALUES(3)", asList(emptyList()));
    runner.batchInsert("INSERT INTO a VALUES(4)", VOID, asList(emptyList()));
    runner.tx((qr, tx) -> {
      qr.insert("INSERT INTO a VALUES(5)", VOID);
    });
    
    List<Long> ids = runner.select("SELECT * FROM a", handler).get();
    
    assertThat(ids, contains(1L, 2L, 3L, 4L, 5L));
    assertEquals(runnerSync.select("SELECT * FROM a", handler), ids);
    assertEquals(6, executorUsed.intValue());
  }
  
  @Test
  public void should_signal_end_of_transaction() throws Exception {
    AsyncQueryRunner runner = QueryRunner.create(connection.get()).toAsync();
    
    CompletableFuture<Void> future = runner.tx((qr, tx) -> {});
    
    assertNull(future.get());
  }
  
  @Test
  public void should_signal_error_in_transaction() throws Exception {
    AsyncQueryRunner runner = QueryRunner.create(connection.get()).toAsync();
    
    CompletableFuture<Void> future = runner.tx((qr, tx) -> { throw new Exception("test"); });
    
    exception.expectCause(isA(Exception.class));
    exception.expectMessage("test");
    
    future.get();
  }
  
}
