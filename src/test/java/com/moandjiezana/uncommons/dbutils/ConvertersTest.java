package com.moandjiezana.uncommons.dbutils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ConvertersTest {
  
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void should_fail_if_converting_to_unknown_type() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(Runnable.class.getName());

    Converters.INSTANCE.convert(Runnable.class, "abc");
  }
  
  @Test
  public void should_convert_clob_to_string() throws Exception {
    String result = Converters.INSTANCE.convert(String.class, new SimpleClob("some text"));
    
    Assert.assertEquals("some text", result);
  }
  
  private static class SimpleClob implements Clob {
    
    private final String content;

    public SimpleClob(String content) {
      this.content = content;
    }

    @Override
    public long length() throws SQLException {
      return content.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
      return content.substring((int) pos, (int) pos + length);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
      return new StringReader(content);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
      return new ByteArrayInputStream(content.getBytes());
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
      return content.indexOf(searchstr, (int) start);
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
      return 0;
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
      return 0;
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
      return 0;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
      return null;
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
      return null;
    }

    @Override
    public void truncate(long len) throws SQLException {}

    @Override
    public void free() throws SQLException {}

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
      return null;
    }
    
  }
}
