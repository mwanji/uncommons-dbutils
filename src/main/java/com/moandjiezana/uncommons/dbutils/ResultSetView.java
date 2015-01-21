package com.moandjiezana.uncommons.dbutils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ResultSetView implements ResultSet {
  
  private class ResultSetMetadata implements ResultSetMetaData {
    private final ResultSetMetaData rsmd;
    private final int columnCount = mapping.length - 1;

    public ResultSetMetadata(ResultSetMetaData rsmd) {
      this.rsmd = rsmd;
    }
    
    @Override
    public int getColumnCount() throws SQLException {
      return columnCount;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return rsmd.unwrap(iface);
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
      return rsmd.isAutoIncrement(mapping[column]);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
      return rsmd.isCaseSensitive(mapping[column]);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
      return rsmd.isSearchable(mapping[column]);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return rsmd.isWrapperFor(iface);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
      return rsmd.isCurrency(mapping[column]);
    }

    @Override
    public int isNullable(int column) throws SQLException {
      return rsmd.isNullable(mapping[column]);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
      return rsmd.isSigned(mapping[column]);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
      return rsmd.getColumnDisplaySize(mapping[column]);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
      return rsmd.getColumnLabel(mapping[column]);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
      return rsmd.getColumnName(mapping[column]);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
      return rsmd.getSchemaName(mapping[column]);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
      return rsmd.getPrecision(mapping[column]);
    }

    @Override
    public int getScale(int column) throws SQLException {
      return rsmd.getScale(mapping[column]);
    }

    @Override
    public String getTableName(int column) throws SQLException {
      return rsmd.getTableName(column);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
      return rsmd.getCatalogName(mapping[column]);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
      return rsmd.getColumnType(mapping[column]);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
      return rsmd.getColumnTypeName(mapping[column]);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
      return rsmd.isReadOnly(mapping[column]);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
      return rsmd.isWritable(mapping[column]);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
      return rsmd.isDefinitelyWritable(mapping[column]);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
      return rsmd.getColumnClassName(mapping[column]);
    }
  }
  
  private final ResultSet rs;
  private final String table;
  private final int[] mapping;
  private final Map<String, Integer> labelMapping = new HashMap<>();
  private final ResultSetMetadata rsmd;

  public ResultSetView(ResultSet rs, String table) {
    this.rs = rs;
    this.table = table;
    List<Integer> tempMapping = new ArrayList<>();
    tempMapping.add(-1);
    try {
      for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
        if (rs.getMetaData().getTableName(i).equalsIgnoreCase(table)) {
          tempMapping.add(i);
          labelMapping.put(rs.getMetaData().getColumnLabel(i).toLowerCase(), i);
        }
      }
      this.mapping = tempMapping.stream().mapToInt(Integer::intValue).toArray();
      this.rsmd = new ResultSetView.ResultSetMetadata(rs.getMetaData());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return rsmd;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return rs.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return rs.isWrapperFor(iface);
  }

  @Override
  public boolean next() throws SQLException {
    return rs.next();
  }

  @Override
  public void close() throws SQLException {
    rs.close();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return rs.wasNull();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return rs.getString(mapping[columnIndex]);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return rs.getBoolean(mapping[columnIndex]);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return rs.getByte(mapping[columnIndex]);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return rs.getShort(mapping[columnIndex]);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return rs.getInt(mapping[columnIndex]);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return rs.getLong(mapping[columnIndex]);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return rs.getFloat(mapping[columnIndex]);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return rs.getDouble(mapping[columnIndex]);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return rs.getBigDecimal(mapping[columnIndex], scale);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return rs.getBytes(mapping[columnIndex]);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return rs.getDate(mapping[columnIndex]);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return rs.getTime(mapping[columnIndex]);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return rs.getTimestamp(mapping[columnIndex]);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return rs.getAsciiStream(mapping[columnIndex]);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return rs.getUnicodeStream(mapping[columnIndex]);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return rs.getBinaryStream(mapping[columnIndex]);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return rs.getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return rs.getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return rs.getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return rs.getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return rs.getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return rs.getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return rs.getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return rs.getDouble(findColumn(columnLabel));
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return rs.getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return rs.getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return rs.getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return rs.getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return rs.getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return rs.getAsciiStream(findColumn(columnLabel));
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return rs.getUnicodeStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return rs.getBinaryStream(findColumn(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return rs.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    rs.clearWarnings();
  }

  @Override
  public String getCursorName() throws SQLException {
    return rs.getCursorName();
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return rs.getObject(mapping[columnIndex]);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return rs.getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    if (!labelMapping.containsKey(columnLabel.toLowerCase())) {
      throw new SQLException("Could not find column " + columnLabel + " in table " + table);
    }
    
    return labelMapping.get(columnLabel);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return rs.getCharacterStream(mapping[columnIndex]);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return rs.getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return rs.getBigDecimal(mapping[columnIndex]);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return rs.getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return rs.isBeforeFirst();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return rs.isAfterLast();
  }

  @Override
  public boolean isFirst() throws SQLException {
    return rs.isFirst();
  }

  @Override
  public boolean isLast() throws SQLException {
    return rs.isLast();
  }

  @Override
  public void beforeFirst() throws SQLException {
    rs.beforeFirst();
  }

  @Override
  public void afterLast() throws SQLException {
    rs.afterLast();
  }

  @Override
  public boolean first() throws SQLException {
    return rs.first();
  }

  @Override
  public boolean last() throws SQLException {
    return rs.last();
  }

  @Override
  public int getRow() throws SQLException {
    return rs.getRow();
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    return rs.absolute(row);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    return rs.relative(rows);
  }

  @Override
  public boolean previous() throws SQLException {
    return rs.previous();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    rs.setFetchDirection(direction);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return rs.getFetchDirection();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    rs.setFetchSize(rows);
  }

  @Override
  public int getFetchSize() throws SQLException {
    return rs.getFetchSize();
  }

  @Override
  public int getType() throws SQLException {
    return rs.getType();
  }

  @Override
  public int getConcurrency() throws SQLException {
    return rs.getConcurrency();
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return rs.rowUpdated();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return rs.rowInserted();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return rs.rowDeleted();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    rs.updateNull(mapping[columnIndex]);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    rs.updateBoolean(mapping[columnIndex], x);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    rs.updateByte(mapping[columnIndex], x);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    rs.updateShort(mapping[columnIndex], x);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    rs.updateInt(mapping[columnIndex], x);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    rs.updateLong(mapping[columnIndex], x);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    rs.updateFloat(mapping[columnIndex], x);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    rs.updateDouble(mapping[columnIndex], x);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    rs.updateBigDecimal(mapping[columnIndex], x);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    rs.updateString(mapping[columnIndex], x);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    rs.updateBytes(mapping[columnIndex], x);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    rs.updateDate(mapping[columnIndex], x);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    rs.updateTime(mapping[columnIndex], x);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    rs.updateTimestamp(mapping[columnIndex], x);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    rs.updateAsciiStream(mapping[columnIndex], x, length);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    rs.updateBinaryStream(mapping[columnIndex], x, length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    rs.updateCharacterStream(mapping[columnIndex], x, length);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    rs.updateObject(mapping[columnIndex], x, scaleOrLength);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    rs.updateObject(mapping[columnIndex], x);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    rs.updateNull(findColumn(columnLabel));
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    rs.updateBoolean(findColumn(columnLabel), x);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    rs.updateByte(findColumn(columnLabel), x);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    rs.updateShort(findColumn(columnLabel), x);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    rs.updateInt(findColumn(columnLabel), x);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    rs.updateLong(findColumn(columnLabel), x);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    rs.updateFloat(findColumn(columnLabel), x);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    rs.updateDouble(findColumn(columnLabel), x);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    rs.updateBigDecimal(findColumn(columnLabel), x);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    rs.updateString(findColumn(columnLabel), x);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    rs.updateBytes(findColumn(columnLabel), x);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    rs.updateDate(findColumn(columnLabel), x);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    rs.updateTime(findColumn(columnLabel), x);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    rs.updateTimestamp(findColumn(columnLabel), x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    rs.updateAsciiStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    rs.updateBinaryStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    rs.updateCharacterStream(findColumn(columnLabel), reader, length);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    rs.updateObject(findColumn(columnLabel), x, scaleOrLength);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    rs.updateObject(findColumn(columnLabel), x);
  }

  @Override
  public void insertRow() throws SQLException {
    rs.insertRow();
  }

  @Override
  public void updateRow() throws SQLException {
    rs.updateRow();
  }

  @Override
  public void deleteRow() throws SQLException {
    rs.deleteRow();
  }

  @Override
  public void refreshRow() throws SQLException {
    rs.refreshRow();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    rs.cancelRowUpdates();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    rs.moveToInsertRow();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    rs.moveToCurrentRow();
  }

  @Override
  public Statement getStatement() throws SQLException {
    return rs.getStatement();
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return rs.getObject(mapping[columnIndex], map);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return rs.getRef(mapping[columnIndex]);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return rs.getBlob(mapping[columnIndex]);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return rs.getClob(mapping[columnIndex]);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return rs.getArray(mapping[columnIndex]);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return rs.getObject(findColumn(columnLabel), map);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return rs.getRef(findColumn(columnLabel));
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return rs.getBlob(findColumn(columnLabel));
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return rs.getClob(findColumn(columnLabel));
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return rs.getArray(findColumn(columnLabel));
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return rs.getDate(mapping[columnIndex], cal);
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return rs.getDate(findColumn(columnLabel), cal);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return rs.getTime(mapping[columnIndex], cal);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return rs.getTime(findColumn(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return rs.getTimestamp(columnIndex, cal);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return rs.getTimestamp(findColumn(columnLabel), cal);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return rs.getURL(mapping[columnIndex]);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return rs.getURL(findColumn(columnLabel));
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    rs.updateRef(mapping[columnIndex], x);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    rs.updateRef(findColumn(columnLabel), x);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    rs.updateBlob(mapping[columnIndex], x);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    rs.updateBlob(findColumn(columnLabel), x);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    rs.updateClob(mapping[columnIndex], x);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    rs.updateClob(findColumn(columnLabel), x);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    rs.updateArray(mapping[columnIndex], x);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    rs.updateArray(findColumn(columnLabel), x);
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return rs.getRowId(mapping[columnIndex]);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return rs.getRowId(findColumn(columnLabel));
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    rs.updateRowId(mapping[columnIndex], x);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    rs.updateRowId(findColumn(columnLabel), x);
  }

  @Override
  public int getHoldability() throws SQLException {
    return rs.getHoldability();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return rs.isClosed();
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    rs.updateNString(mapping[columnIndex], nString);
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    rs.updateNString(findColumn(columnLabel), nString);
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    rs.updateNClob(mapping[columnIndex], nClob);
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    rs.updateNClob(findColumn(columnLabel), nClob);
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return rs.getNClob(mapping[columnIndex]);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return rs.getNClob(findColumn(columnLabel));
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return rs.getSQLXML(mapping[columnIndex]);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return rs.getSQLXML(findColumn(columnLabel));
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    rs.updateSQLXML(mapping[columnIndex], xmlObject);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    rs.updateSQLXML(findColumn(columnLabel), xmlObject);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return rs.getNString(mapping[columnIndex]);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return rs.getNString(findColumn(columnLabel));
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return rs.getNCharacterStream(mapping[columnIndex]);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return rs.getNCharacterStream(findColumn(columnLabel));
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    rs.updateNCharacterStream(mapping[columnIndex], x, length);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    rs.updateNCharacterStream(findColumn(columnLabel), reader, length);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    rs.updateAsciiStream(mapping[columnIndex], x, length);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    rs.updateBinaryStream(mapping[columnIndex], x, length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    rs.updateCharacterStream(mapping[columnIndex], x, length);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    rs.updateAsciiStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    rs.updateBinaryStream(findColumn(columnLabel), x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    rs.updateCharacterStream(findColumn(columnLabel), reader, length);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    rs.updateBlob(mapping[columnIndex], inputStream, length);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    rs.updateBlob(findColumn(columnLabel), inputStream, length);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    rs.updateClob(mapping[columnIndex], reader, length);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    rs.updateClob(findColumn(columnLabel), reader, length);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    rs.updateNClob(mapping[columnIndex], reader, length);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    rs.updateNClob(findColumn(columnLabel), reader, length);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    rs.updateNCharacterStream(mapping[columnIndex], x);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    rs.updateNCharacterStream(findColumn(columnLabel), reader);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    rs.updateAsciiStream(mapping[columnIndex], x);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    rs.updateBinaryStream(mapping[columnIndex], x);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    rs.updateCharacterStream(mapping[columnIndex], x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    rs.updateAsciiStream(findColumn(columnLabel), x);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    rs.updateBinaryStream(findColumn(columnLabel), x);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    rs.updateCharacterStream(findColumn(columnLabel), reader);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    rs.updateBlob(mapping[columnIndex], inputStream);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    rs.updateBlob(findColumn(columnLabel), inputStream);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    rs.updateClob(mapping[columnIndex], reader);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    rs.updateClob(findColumn(columnLabel), reader);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    rs.updateNClob(mapping[columnIndex], reader);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    rs.updateNClob(findColumn(columnLabel), reader);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return rs.getObject(mapping[columnIndex], type);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return rs.getObject(findColumn(columnLabel), type);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    rs.updateObject(mapping[columnIndex], x, targetSqlType, scaleOrLength);
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
    rs.updateObject(findColumn(columnLabel), x, targetSqlType, scaleOrLength);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    rs.updateObject(mapping[columnIndex], x, targetSqlType);
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
    rs.updateObject(findColumn(columnLabel), x, targetSqlType);
  }
}
