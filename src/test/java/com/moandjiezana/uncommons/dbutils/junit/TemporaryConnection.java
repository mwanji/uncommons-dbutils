package com.moandjiezana.uncommons.dbutils.junit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.rules.ExternalResource;

public class TemporaryConnection extends ExternalResource {

  private final String url;
  private Connection connection;
  
  public TemporaryConnection(String url) {
    this.url = url;
  }

  @Override
  protected void before() throws Throwable {
    connection = DriverManager.getConnection(url);
  }
  
  @Override
  protected void after() {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public Connection get() {
    return connection;
  };
}
