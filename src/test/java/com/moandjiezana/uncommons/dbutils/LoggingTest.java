package com.moandjiezana.uncommons.dbutils;

import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.VOID;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.org.lidalia.slf4jtest.LoggingEvent.debug;
import static uk.org.lidalia.slf4jtest.LoggingEvent.error;
import static uk.org.lidalia.slf4jtest.LoggingEvent.trace;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.junit.Rule;
import org.junit.Test;

import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;
import uk.org.lidalia.slf4jtest.TestLoggerFactoryResetRule;

import com.moandjiezana.uncommons.dbutils.junit.TemporaryConnection;

public class LoggingTest {
  
  private static final String LOG_MESSAGE_FORMAT = "QUERY: {}; VALUES: {};";

  @Rule
  public TestLoggerFactoryResetRule resetLogger = new TestLoggerFactoryResetRule();
  
  @Rule
  public TemporaryConnection connection = new TemporaryConnection("jdbc:h2:mem:");
  
  private final TestLogger logger = TestLoggerFactory.getTestLogger(QueryRunner.class);

  @Test
  public void should_log_creation() throws Exception {
    QueryRunner.create(connection.get());
    
    assertThat(logger.getLoggingEvents(), contains(trace("QueryRunner connected to", connection.get().getMetaData().getURL())));
  }
  
  @Test
  public void should_log_select() throws Exception {
    QueryRunner queryRunner = QueryRunner.create(connection.get());
    queryRunner.select("SELECT * WHERE 1 = ? or 2 = ?", VOID, 1L, 2L);
    
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("SELECT * WHERE 1 = ? or 2 = ?", "[1, 2]")));
  }
  
  @Test
  public void should_log_execute() throws Exception {
    QueryRunner queryRunner = QueryRunner.create(connection.get());
    queryRunner.execute("SET MODE REGULAR");
    
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("SET MODE REGULAR", "[]")));
  }
  
  @Test
  public void should_log_insert() throws Exception {
    QueryRunner queryRunner = QueryRunner.create(connection.get());
    queryRunner.execute("CREATE TABLE a (id IDENTITY)");
    queryRunner.insert("INSERT INTO a VALUES(?)", VOID, 3L);
    
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("INSERT INTO a VALUES(?)", "[3]")));
  }
  
  @Test
  public void should_log_execute_batch() throws Exception {
    QueryRunner queryRunner = QueryRunner.create(connection.get());
    queryRunner.execute("CREATE TABLE a (id IDENTITY, name VARCHAR(255))");
    queryRunner.batch("INSERT INTO a VALUES(?,?)", asList(asList(1L, "a"), asList(2L, "b"), asList(3L, "c")));
    
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("INSERT INTO a VALUES(?,?)", "[1, 'a']")));
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("INSERT INTO a VALUES(?,?)", "[2, 'b']")));
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("INSERT INTO a VALUES(?,?)", "[3, 'c']")));
  }
  
  @Test
  public void should_log_insert_batch() throws Exception {
    QueryRunner queryRunner = QueryRunner.create(connection.get());
    queryRunner.execute("CREATE TABLE a (id IDENTITY, name VARCHAR(255))");
    queryRunner.batchInsert("INSERT INTO a VALUES(?,?)", VOID, asList(asList(1L, "a"), asList(2L, "b"), asList(3L, "c")));
    
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("INSERT INTO a VALUES(?,?)", "[1, 'a']")));
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("INSERT INTO a VALUES(?,?)", "[2, 'b']")));
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("INSERT INTO a VALUES(?,?)", "[3, 'c']")));
  }
  
  @Test
  public void should_log_transaction() throws Exception {
    QueryRunner queryRunner = QueryRunner.create(connection.get());
    queryRunner.tx((qr, tx) -> {
      qr.select("SELECT 1", VOID);
      tx.rollback();
      qr.execute("SET MODE REGULAR");
      tx.commit();
    });
    
    assertThat(logger.getLoggingEvents(), contains(
      trace("QueryRunner connected to", connection.get().getMetaData().getURL()),
      debug("Transaction: START"),
      debugMsg("SELECT 1", "[]"),
      debug("Transaction: ROLLBACK"),
      debugMsg("SET MODE REGULAR", "[]"),
      debug("Transaction: COMMIT"),
      debug("Transaction: END")
    ));
  }
  
  @Test
  public void should_log_error_if_connection_creation_log_fails() throws Exception {
    Connection connection = mock(Connection.class);
    DatabaseMetaData databaseMetadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(databaseMetadata);
    when(databaseMetadata.getURL()).thenThrow(new SQLException("test"));
    
    QueryRunner.create(connection);
    
    assertThat(logger.getLoggingEvents(), contains(error("Could not log start-up message: {}", "test")));
  }
  
  @Test
  public void should_handle_null_params() throws Exception {
    QueryRunner queryRunner = QueryRunner.create(connection.get());
    queryRunner.select("SELECT * WHERE 1 = ? or 2 = ?", VOID, 1L, null);
    
    assertThat(logger.getLoggingEvents(), hasItem(debugMsg("SELECT * WHERE 1 = ? or 2 = ?", "[1, null]")));
  }
  
  private LoggingEvent debugMsg(Object... values) {
    return debug(LOG_MESSAGE_FORMAT, values);
  }
}
