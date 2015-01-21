package com.moandjiezana.uncommons.dbutils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

import org.junit.Test;

public class ResultSetViewTest {

  @Test
  @SuppressWarnings("deprecation")
  public void should_delegate_to_underlying_ResultSet() throws Exception {
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);
    ResultSet delegate = mock(ResultSet.class);
    when(rsmd.getTableName(1)).thenReturn("table");
    when(rsmd.getColumnCount()).thenReturn(1);
    when(rsmd.getColumnLabel(1)).thenReturn("col");
    when(delegate.getMetaData()).thenReturn(rsmd);
    
    try (ResultSet rs = new ResultSetView(delegate, "table")) {
      rs.unwrap(Object.class);
      verify(delegate).unwrap(Object.class);
      
      rs.isWrapperFor(Object.class);
      verify(delegate).isWrapperFor(Object.class);
      
      rs.next();
      verify(delegate).next();
      
      rs.wasNull();
      verify(delegate).wasNull();
      
      rs.getString(1);
      rs.getString("col");
      verify(delegate, times(2)).getString(1);
      
      rs.getNString(1);
      rs.getNString("col");
      verify(delegate, times(2)).getNString(1);
      
      rs.getBoolean(1);
      rs.getBoolean("col");
      verify(delegate, times(2)).getBoolean(1);
      
      rs.getByte(1);
      rs.getByte("col");
      verify(delegate, times(2)).getByte(1);
      
      rs.getShort(1);
      rs.getShort("col");
      verify(delegate, times(2)).getShort(1);
      
      rs.getInt(1);
      rs.getInt("col");
      verify(delegate, times(2)).getInt(1);
      
      rs.getLong(1);
      rs.getLong("col");
      verify(delegate, times(2)).getLong(1);
      
      rs.getFloat(1);
      rs.getFloat("col");
      verify(delegate, times(2)).getFloat(1);
      
      rs.getDouble(1);
      rs.getDouble("col");
      verify(delegate, times(2)).getDouble(1);
      
      rs.getBigDecimal(1);
      rs.getBigDecimal("col");
      verify(delegate, times(2)).getBigDecimal(1);
      
      rs.getBigDecimal(1, 3);
      rs.getBigDecimal("col", 3);
      verify(delegate, times(2)).getBigDecimal(1, 3);
      
      rs.getBytes(1);
      rs.getBytes("col");
      verify(delegate, times(2)).getBytes(1);
      
      rs.getDate(1);
      rs.getDate("col");
      verify(delegate, times(2)).getDate(1);
      
      Calendar cal = Calendar.getInstance();

      rs.getDate(1, cal);
      rs.getDate("col", cal);
      verify(delegate, times(2)).getDate(1, cal);
      
      rs.getTime(1);
      rs.getTime("col");
      verify(delegate, times(2)).getTime(1);
      
      rs.getTime(1, cal);
      rs.getTime("col", cal);
      verify(delegate, times(2)).getTime(1, cal);
      
      rs.getTimestamp(1);
      rs.getTimestamp("col");
      verify(delegate, times(2)).getTimestamp(1);
      
      rs.getTimestamp(1, cal);
      rs.getTimestamp("col", cal);
      verify(delegate, times(2)).getTimestamp(1, cal);
      
      rs.getAsciiStream(1);
      rs.getAsciiStream("col");
      verify(delegate, times(2)).getAsciiStream(1);
      
      rs.getUnicodeStream(1);
      rs.getUnicodeStream("col");
      verify(delegate, times(2)).getUnicodeStream(1);
      
      rs.getBinaryStream(1);
      rs.getBinaryStream("col");
      verify(delegate, times(2)).getBinaryStream(1);
      
      rs.getURL(1);
      rs.getURL("col");
      verify(delegate, times(2)).getURL(1);
      
      rs.getClob(1);
      rs.getClob("col");
      verify(delegate, times(2)).getClob(1);
      
      rs.getNClob(1);
      rs.getNClob("col");
      verify(delegate, times(2)).getNClob(1);
      
      rs.getBlob(1);
      rs.getBlob("col");
      verify(delegate, times(2)).getBlob(1);
      
      rs.getArray(1);
      rs.getArray("col");
      verify(delegate, times(2)).getArray(1);

      rs.getRef(1);
      rs.getRef("col");
      verify(delegate, times(2)).getRef(1);
      
      rs.getObject(1, Collections.emptyMap());
      rs.getObject("col", Collections.emptyMap());
      verify(delegate, times(2)).getObject(1, Collections.emptyMap());
      
      rs.getObject(1, String.class);
      rs.getObject("col", String.class);
      verify(delegate, times(2)).getObject(1, String.class);
      
      rs.getWarnings();
      verify(delegate).getWarnings();
      
      rs.clearWarnings();
      verify(delegate).clearWarnings();
      
      rs.getCursorName();
      verify(delegate).getCursorName();

      rs.getCharacterStream(1);
      rs.getCharacterStream("col");
      verify(delegate, times(2)).getCharacterStream(1);
      
      rs.getNCharacterStream(1);
      rs.getNCharacterStream("col");
      verify(delegate, times(2)).getNCharacterStream(1);
      
      rs.getSQLXML(1);
      rs.getSQLXML("col");
      verify(delegate, times(2)).getSQLXML(1);
      
      rs.isBeforeFirst();
      verify(delegate).isBeforeFirst();
      
      rs.isAfterLast();
      verify(delegate).isAfterLast();
      
      rs.isFirst();
      verify(delegate).isFirst();
      
      rs.isLast();
      verify(delegate).isLast();
      
      rs.beforeFirst();
      verify(delegate).beforeFirst();
      
      rs.afterLast();
      verify(delegate).afterLast();
      
      rs.first();
      verify(delegate).first();
      
      rs.last();
      verify(delegate).last();
      
      rs.getRow();
      verify(delegate).getRow();
      
      rs.absolute(1);
      verify(delegate).absolute(1);
      
      rs.relative(2);
      verify(delegate).relative(2);
      
      rs.previous();
      verify(delegate).previous();
      
      rs.setFetchDirection(1);
      verify(delegate).setFetchDirection(1);
      
      rs.getFetchDirection();
      verify(delegate).getFetchDirection();
      
      rs.getFetchSize();
      verify(delegate).getFetchSize();
      
      rs.setFetchSize(3);
      verify(delegate).setFetchSize(3);
      
      rs.getType();
      verify(delegate).getType();
      
      rs.getConcurrency();
      verify(delegate).getConcurrency();
      
      rs.getRowId(1);
      rs.getRowId("col");
      verify(delegate, times(2)).getRowId(1);
      
      rs.rowUpdated();
      verify(delegate).rowUpdated();
      
      rs.rowInserted();
      verify(delegate).rowInserted();
      
      rs.rowDeleted();
      verify(delegate).rowDeleted();
      
      rs.updateNull(1);
      rs.updateNull("col");
      verify(delegate, times(2)).updateNull(1);
      
      rs.updateString(1, "abc");
      rs.updateString("col", "abc");
      verify(delegate, times(2)).updateString(1, "abc");
      
      rs.updateNString(1, "abc");
      rs.updateNString("col", "abc");
      verify(delegate, times(2)).updateNString(1, "abc");
      
      rs.updateBoolean(1, true);
      rs.updateBoolean("col", true);
      verify(delegate, times(2)).updateBoolean(1, true);
      
      rs.updateByte(1, (byte) 1);
      rs.updateByte("col", (byte) 1);
      verify(delegate, times(2)).updateByte(1, (byte) 1);
      
      rs.updateBytes(1, "a".getBytes());
      rs.updateBytes("col", "a".getBytes());
      verify(delegate, times(2)).updateBytes(1, "a".getBytes());
      
      rs.updateShort(1, (short) 128);
      rs.updateShort("col", (short) 128);
      verify(delegate, times(2)).updateShort(1, (short) 128);
      
      rs.updateInt(1, 5);
      rs.updateInt("col", 5);
      verify(delegate, times(2)).updateInt(1, 5);
      
      rs.updateLong(1, 6);
      rs.updateLong("col", 6);
      verify(delegate, times(2)).updateLong(1, 6);
      
      rs.updateFloat(1, 5.3f);
      rs.updateFloat("col", 5.3f);
      verify(delegate, times(2)).updateFloat(1, 5.3f);
      
      rs.updateDouble(1, 5.1);
      rs.updateDouble("col", 5.1);
      verify(delegate, times(2)).updateDouble(1, 5.1);
      
      rs.updateBigDecimal(1, BigDecimal.ONE);
      rs.updateBigDecimal("col", BigDecimal.ONE);
      verify(delegate, times(2)).updateBigDecimal(1, BigDecimal.ONE);
      
      rs.updateObject(1, BigDecimal.ONE);
      rs.updateObject("col", BigDecimal.ONE);
      verify(delegate, times(2)).updateObject(1, BigDecimal.ONE);
      
      rs.updateObject(1, BigDecimal.TEN, JDBCType.NUMERIC);
      rs.updateObject("col", BigDecimal.TEN, JDBCType.NUMERIC);
      verify(delegate, times(2)).updateObject(1, BigDecimal.TEN, JDBCType.NUMERIC);
      
      rs.updateObject(1, BigDecimal.TEN, 0);
      rs.updateObject("col", BigDecimal.TEN, 0);
      verify(delegate, times(2)).updateObject(1, BigDecimal.TEN, 0);

      rs.updateObject(1, BigDecimal.TEN, JDBCType.NUMERIC, 0);
      rs.updateObject("col", BigDecimal.TEN, JDBCType.NUMERIC, 0);
      verify(delegate, times(2)).updateObject(1, BigDecimal.TEN, JDBCType.NUMERIC, 0);
      
      java.sql.Date now = new java.sql.Date(new Date().getTime());
      rs.updateDate(1, now);
      rs.updateDate("col", now);
      verify(delegate, times(2)).updateDate(1, now);
      
      Time timeNow = new Time(new Date().getTime());
      rs.updateTime(1, timeNow);
      rs.updateTime("col", timeNow);
      verify(delegate, times(2)).updateTime(1, timeNow);
      
      Timestamp timestamp = new Timestamp(new Date().getTime());
      rs.updateTimestamp(1, timestamp);
      rs.updateTimestamp("col", timestamp);
      verify(delegate, times(2)).updateTimestamp(1, timestamp);
      
      Ref ref = mock(Ref.class);
      rs.updateRef(1, ref);
      rs.updateRef("col", ref);
      verify(delegate, times(2)).updateRef(1, ref);
      
      SQLXML sqlXml = mock(SQLXML.class);
      rs.updateSQLXML(1, sqlXml);
      rs.updateSQLXML("col", sqlXml);
      verify(delegate, times(2)).updateSQLXML(1, sqlXml);
      
      try (ByteArrayInputStream inputStream = new ByteArrayInputStream("text".getBytes()); Reader reader = new InputStreamReader(inputStream)) {
        rs.updateAsciiStream(1, inputStream);
        rs.updateAsciiStream("col", inputStream);
        verify(delegate, times(2)).updateAsciiStream(1, inputStream);
        
        rs.updateAsciiStream(1, inputStream, 4);
        rs.updateAsciiStream("col", inputStream, 4);
        verify(delegate, times(2)).updateAsciiStream(1, inputStream, 4);
        
        rs.updateAsciiStream(1, inputStream, 4L);
        rs.updateAsciiStream("col", inputStream, 4L);
        verify(delegate, times(2)).updateAsciiStream(1, inputStream, 4L);
        
        rs.updateBinaryStream(1, inputStream);
        rs.updateBinaryStream("col", inputStream);
        verify(delegate, times(2)).updateBinaryStream(1, inputStream);
        
        rs.updateBinaryStream(1, inputStream, 4);
        rs.updateBinaryStream("col", inputStream, 4);
        verify(delegate, times(2)).updateBinaryStream(1, inputStream, 4);
        
        rs.updateBinaryStream(1, inputStream, 4L);
        rs.updateBinaryStream("col", inputStream, 4L);
        verify(delegate, times(2)).updateBinaryStream(1, inputStream, 4L);
        
        rs.updateCharacterStream(1, reader);
        rs.updateCharacterStream("col", reader);
        verify(delegate, times(2)).updateCharacterStream(1, reader);
        
        rs.updateCharacterStream(1, reader, 4);
        rs.updateCharacterStream("col", reader, 4);
        verify(delegate, times(2)).updateCharacterStream(1, reader, 4);
        
        rs.updateCharacterStream(1, reader, 4L);
        rs.updateCharacterStream("col", reader, 4L);
        verify(delegate, times(2)).updateCharacterStream(1, reader, 4L);
        
        rs.updateNCharacterStream(1, reader);
        rs.updateNCharacterStream("col", reader);
        verify(delegate, times(2)).updateNCharacterStream(1, reader);
        
        rs.updateNCharacterStream(1, reader, 4L);
        rs.updateNCharacterStream("col", reader, 4L);
        verify(delegate, times(2)).updateNCharacterStream(1, reader, 4L);
        
        Blob blob = mock(Blob.class);
        rs.updateBlob(1, blob);
        rs.updateBlob("col", blob);
        verify(delegate, times(2)).updateBlob(1, blob);
        
        rs.updateBlob(1, inputStream);
        rs.updateBlob("col", inputStream);
        verify(delegate, times(2)).updateBlob(1, inputStream);
        
        rs.updateBlob(1, inputStream, 4L);
        rs.updateBlob("col", inputStream, 4L);
        verify(delegate, times(2)).updateBlob(1, inputStream, 4L);
        
        Clob clob = mock(Clob.class);
        rs.updateClob(1, clob);
        rs.updateClob("col", clob);
        verify(delegate, times(2)).updateClob(1, clob);
        
        rs.updateClob(1, reader);
        rs.updateClob("col", reader);
        verify(delegate, times(2)).updateClob(1, reader);
        
        rs.updateClob(1, reader, 4L);
        rs.updateClob("col", reader, 4L);
        verify(delegate, times(2)).updateClob(1, reader, 4L);
        
        rs.updateNClob(1, reader, 4L);
        rs.updateNClob("col", reader, 4L);
        verify(delegate, times(2)).updateNClob(1, reader, 4L);
        
        NClob nclob = mock(NClob.class);
        
        rs.updateNClob(1, reader);
        rs.updateNClob("col", reader);
        verify(delegate, times(2)).updateNClob(1, reader);
        
        rs.updateNClob(1, nclob);
        rs.updateNClob("col", nclob);
        verify(delegate, times(2)).updateNClob(1, nclob);

        rs.updateClob(1, nclob);
        rs.updateClob("col", nclob);
        verify(delegate, times(2)).updateClob(1, nclob);
        
        Array array = mock(Array.class);
        rs.updateArray(1, array);
        rs.updateArray("col", array);
        verify(delegate, times(2)).updateArray(1, array);
        
        RowId rowId = mock(RowId.class);
        rs.updateRowId(1, rowId);
        rs.updateRowId("col", rowId);
        verify(delegate, times(2)).updateRowId(1, rowId);
      }
      
      
      rs.insertRow();
      verify(delegate).insertRow();
      
      rs.updateRow();
      verify(delegate).updateRow();
      
      rs.deleteRow();
      verify(delegate).deleteRow();
      
      rs.refreshRow();
      verify(delegate).refreshRow();
      
      rs.cancelRowUpdates();
      verify(delegate).cancelRowUpdates();
      
      rs.moveToInsertRow();
      verify(delegate).moveToInsertRow();
      
      rs.moveToCurrentRow();
      verify(delegate).moveToCurrentRow();
      
      rs.getStatement();
      verify(delegate).getStatement();
      
      rs.getHoldability();
      verify(delegate).getHoldability();
      
      rs.isClosed();
      verify(delegate).isClosed();
    }
    
    verify(delegate).close();
  }
  
  @Test
  public void should_delegate_to_underlying_ResultSetMetaData() throws Exception {
    ResultSet resultSet = mock(ResultSet.class);
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(resultSet.getMetaData()).thenReturn(metaData);
    when(metaData.getColumnCount()).thenReturn(1);
    when(metaData.getTableName(1)).thenReturn("table");
    when(metaData.getColumnLabel(1)).thenReturn("col");
    
    ResultSetMetaData viewMetaData = new ResultSetView(resultSet, "table").getMetaData();
    
    verify(metaData).getColumnLabel(1);
    
    viewMetaData.getTableName(1);
    verify(metaData, times(2)).getTableName(1);
    
    viewMetaData.unwrap(Object.class);
    verify(metaData).unwrap(Object.class);
    
    viewMetaData.isAutoIncrement(1);
    verify(metaData).isAutoIncrement(1);
    
    viewMetaData.isCaseSensitive(1);
    verify(metaData).isCaseSensitive(1);
    
    viewMetaData.isSearchable(1);
    verify(metaData).isSearchable(1);
    
    viewMetaData.isWrapperFor(Object.class);
    verify(metaData).isWrapperFor(Object.class);
    
    viewMetaData.isCurrency(1);
    verify(metaData).isCurrency(1);
    
    viewMetaData.isNullable(1);
    verify(metaData).isNullable(1);

    viewMetaData.isSigned(1);
    verify(metaData).isSigned(1);
    
    viewMetaData.getColumnDisplaySize(1);
    verify(metaData).getColumnDisplaySize(1);
    
    viewMetaData.getColumnName(1);
    verify(metaData).getColumnName(1);
    
    viewMetaData.getSchemaName(1);
    verify(metaData).getSchemaName(1);
    
    viewMetaData.getPrecision(1);
    verify(metaData).getPrecision(1);
    
    viewMetaData.getScale(1);
    verify(metaData).getScale(1);
    
    viewMetaData.getCatalogName(1);
    verify(metaData).getCatalogName(1);
    
    viewMetaData.getColumnType(1);
    verify(metaData).getColumnType(1);
    
    viewMetaData.getColumnTypeName(1);
    verify(metaData).getColumnTypeName(1);
    
    viewMetaData.isReadOnly(1);
    verify(metaData).isReadOnly(1);
    
    viewMetaData.isWritable(1);
    verify(metaData).isWritable(1);
    
    viewMetaData.isDefinitelyWritable(1);
    verify(metaData).isDefinitelyWritable(1);
    
    viewMetaData.getColumnClassName(1);
    verify(metaData).getColumnClassName(1);

  }
}
