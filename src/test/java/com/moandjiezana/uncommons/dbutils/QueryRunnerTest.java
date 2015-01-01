package com.moandjiezana.uncommons.dbutils;

import static com.moandjiezana.uncommons.dbutils.MapRowProcessor.table;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.VOID;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.list;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.map;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.optional;
import static com.moandjiezana.uncommons.dbutils.ResultSetHandler.single;
import static com.moandjiezana.uncommons.dbutils.RowProcessor.firstColumn;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.moandjiezana.uncommons.dbutils.junit.TemporaryConnection;

public class QueryRunnerTest {

  @Rule
  public final TemporaryConnection connection = new TemporaryConnection("jdbc:h2:mem:");
  private QueryRunner queryRunner;
  private final ObjectRowProcessor<Tbl> tblRowProcessor = new ObjectRowProcessor<Tbl>(Tbl.class, ObjectRowProcessor.matching());

  @Before
  public void before() throws Exception {
    queryRunner = QueryRunner.create(connection.get());
    QueryRunner qr = queryRunner;
    prepare(qr);
  }

  @Test
  public void should_get_first_column_of_single_row() throws Exception {
    Long id = queryRunner.insert("INSERT INTO tbl(name) VALUES(?)", single(firstColumn()), "abc1");
    String name = queryRunner.select("SELECT name FROM tbl WHERE id = ?", single(firstColumn()), id);

    assertEquals("abc1", name);
  }

  @Test
  public void should_select_first_column_of_multiple_rows() throws Exception {
    queryRunner.batchInsert("INSERT INTO tbl(name) VALUES(?)", ResultSetHandler.VOID, asList(singletonList("111"), singletonList("222"), singletonList("333")));
    
    List<String> names = queryRunner.select("SELECT name FROM tbl ORDER BY name ASC", list(firstColumn()));
    
    assertThat(names, contains("111", "222", "333"));
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void should_wrap_second_column_in_optional() throws Exception {
    queryRunner.batchInsert("INSERT INTO tbl(name) VALUES(?)", ResultSetHandler.VOID, asList(singletonList("111"), singletonList(null), singletonList("333")));
    
    List<Optional<String>> names = queryRunner.select("SELECT id, name FROM tbl ORDER BY id ASC", list(RowProcessor.optional(new ColumnRowProcessor<String>(2))));
    
    assertThat(names, contains(Optional.of("111"), Optional.empty(), Optional.of("333")));
  }
  
  @Test
  public void should_map_result_set_to_object() throws Exception {
    Instant now = Instant.now();
    Long id = queryRunner.insert("INSERT INTO tbl(name, instant, active, amount, num) VALUES(?,?,?,?,?)", single(firstColumn()), "abc1", Timestamp.from(now), true, BigDecimal.valueOf(1.13), 5);
    
    Tbl tbl = queryRunner.select("SELECT id, name, instant, active, active AS active2, amount, num FROM tbl WHERE id=?", single(tblRowProcessor), id);
    
    assertEquals(1L, tbl.id.longValue());
    assertEquals("abc1", tbl.name);
    assertEquals(now, tbl.instant);
    assertTrue(tbl.active);
    assertTrue(tbl.active2);
    assertEquals(BigDecimal.valueOf(1.13), tbl.amount);
    assertEquals(5, tbl.num);
  }
  
  @Test
  public void should_return_null_for_empty_result_set() throws Exception {
    Tbl tbl = queryRunner.select("SELECT * FROM tbl", single(tblRowProcessor));
    
    assertNull(tbl);
  }
  
  @Test
  public void should_map_result_set_to_objects() throws Exception {
    Instant now = Instant.now();
    Timestamp timestamp = Timestamp.from(now);
    BigDecimal amount = BigDecimal.valueOf(1.13);
    ResultSetHandler<Void> resultSetHandler = ResultSetHandler.VOID;
    queryRunner.insert("INSERT INTO tbl(name, instant, active, amount) VALUES(?,?,?,?)", resultSetHandler, "abc1", timestamp, true, amount);
    queryRunner.insert("INSERT INTO tbl(name, instant, active, amount) VALUES(?,?,?,?)", resultSetHandler, "abc2", timestamp, true, amount);
    queryRunner.insert("INSERT INTO tbl(name, instant, active, amount) VALUES(?,?,?,?)", resultSetHandler, "abc3", timestamp, true, amount);
    
    List<Tbl> tbls = queryRunner.select("SELECT * FROM tbl ORDER BY id ASC", list(tblRowProcessor));
    
    assertThat(tbls, Matchers.hasSize(3));
    
    for (int i = 0; i < tbls.size(); i++) {
      Tbl tbl = tbls.get(i);
      assertEquals("abc" + tbl.id, tbl.name);
    }
  }
  
  @Test
  public void should_map_result_set_with_custom_naming_conventions() throws Exception {
    Instant now = Instant.now();
    Long id = queryRunner.insert("INSERT INTO tbl_underscore(name_of, instant_at, is_active, amount_owed, num_owned) VALUES(?,?,?,?,?)", single(firstColumn()), "abc1", Timestamp.from(now), true, BigDecimal.valueOf(1.13), 5);
    
    TblUnderscore tbl = queryRunner.select("SELECT * FROM tbl_underscore WHERE id_tbl=?", single(new ObjectRowProcessor<TblUnderscore>(TblUnderscore.class, ObjectRowProcessor.underscoresToCamel())), id);
    
    assertEquals(1L, tbl.idTbl.longValue());
    assertEquals("abc1", tbl.nameOf);
    assertEquals(now, tbl.instantAt);
    assertTrue(tbl.isActive);
    assertEquals(BigDecimal.valueOf(1.13), tbl.amountOwed);
    assertEquals(5, tbl.numOwned);
  }
  
  @Test
  public void should_map_result_set_to_case_insensitive_map() throws Exception {
    Instant now = Instant.now();
    Long id = queryRunner.insert("INSERT INTO tbl(name, instant, active, amount, num) VALUES(?,?,?,?,?)", single(firstColumn()), "abc1", Timestamp.from(now), true, BigDecimal.valueOf(1.13), 5);
    
    Map<String, Object> tbl = queryRunner.select("SELECT * FROM tbl WHERE id=?", single(new MapRowProcessor()), id);
    
    assertEquals(Long.valueOf(1), tbl.get("id"));
    assertEquals(Long.valueOf(1), tbl.get("ID"));
    assertEquals("abc1", tbl.get("name"));
    assertEquals(Timestamp.from(now), tbl.get("insTaNt"));
    assertEquals(Boolean.TRUE, tbl.get("active"));
    assertEquals(BigDecimal.valueOf(1.13), tbl.get("amount"));
    assertEquals(5, tbl.get("num"));
  }
  
  @Test
  public void should_map_result_set_to_case_insensitive_maps() throws Exception {
    queryRunner.batch("INSERT INTO tbl(id, name) VALUES(?,?)", asList(asList(1L, "abc1"), asList(2L, "abc2")));
    
    List<Map<String, Object>> tbls = queryRunner.select("SELECT * FROM tbl ORDER BY id", list(new MapRowProcessor()));
    
    assertThat(tbls.stream().map(m -> m.get("name")).collect(toList()), contains("abc1", "abc2"));
  }
  
  @Test
  public void should_return_optional() throws Exception {
    queryRunner.insert("INSERT INTO tbl(id) VALUES(?)", VOID, 1L);
    Optional<Tbl> present = queryRunner.select("SELECT * FROM tbl WHERE id = ?", optional(single(tblRowProcessor)), 1L);
    Optional<Tbl> absent = queryRunner.select("SELECT * FROM tbl WHERE id = ?", optional(single(tblRowProcessor)), 2L);
    
    assertEquals(1L, present.get().id.longValue());
    assertFalse(absent.isPresent());
  }
  
  @Test
  public void should_map_result_set_to_map_of_id_to_object() throws Exception {
    queryRunner.batch("INSERT INTO tbl(name) VALUES(?)", asList(singletonList("a"), singletonList("b")));
    
    Map<Long, Tbl> tbls = queryRunner.select("SELECT * FROM tbl", map("id", Long.class, tblRowProcessor));
        
    assertEquals("a", tbls.get(1L).name);
    assertEquals("b", tbls.get(2L).name);
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void should_map_result_set_to_map_per_table_per_row() throws Exception {
    queryRunner.batch("INSERT INTO tbl(id, name) VALUES(?,?)", asList(asList(1L, "a"), asList(2L, "b")));
    queryRunner.batch("INSERT INTO tbl_underscore(id_tbl, name_of) VALUES(?,?)", asList(asList(1L, "a_"), asList(2L, "b_")));
    
    List<Map<String, Map<String, Object>>> tbls = queryRunner.select("SELECT tbl.id, tbl.name, tbl_underscore.id_tbl, tbl_underscore.name_of FROM tbl INNER JOIN tbl_underscore ON tbl.id = tbl_underscore.id_tbl",
      list(rs -> {
        Map<String, Map<String, Object>> row = new HashMap<>();
        Map<String, MapRowProcessor> rowProcessors = new HashMap<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
          String tableName = rs.getMetaData().getTableName(i).toLowerCase();
          Map<String, Object> tableMap = row.computeIfAbsent(tableName, key -> new HashMap<String, Object>());
          MapRowProcessor rowProcessor = rowProcessors.computeIfAbsent(tableName, key -> new MapRowProcessor(table(tableName)));
          tableMap.putAll(rowProcessor.handle(rs)); 
        }
        
        return row;
      })
    );
    
    HashMap<String, Object> tbl1 = new HashMap<>();
    tbl1.put("id", 1L);
    tbl1.put("name", "a");
    Map<String,Object> tblUnderscore1 = new HashMap<>();
    tblUnderscore1.put("id_tbl", 1L);
    tblUnderscore1.put("name_of", "a_");
    HashMap<String, Map<String, Object>> result1 = new HashMap<>();
    result1.put("tbl", tbl1);
    result1.put("tbl_underscore", tblUnderscore1);
    
    Map<String, Object> tbl2 = new HashMap<>();
    tbl2.put("id", 2L);
    tbl2.put("name", "b");
    Map<String, Object> tblUnderscore2 = new HashMap<>();
    tblUnderscore2.put("id_tbl", 2L);
    tblUnderscore2.put("name_of", "b_");
    Map<String, Map<String, Object>> result2 = new HashMap<>();
    result2.put("tbl", tbl2);
    result2.put("tbl_underscore", tblUnderscore2);
    
    assertThat(tbls, contains(result1, result2));
  }
  
  @Test
  public void should_not_save_if_auto_commit_is_off() throws Exception {
    String url = "jdbc:h2:mem:no_save_if_auto_commit_is_off";
    try (Connection c1 = DriverManager.getConnection(url); Connection c2 = DriverManager.getConnection(url);) {
      QueryRunner queryRunner = QueryRunner.create(c1);
      prepare(queryRunner);

      QueryRunner.create(c1).initializeWith(conn -> conn.setAutoCommit(false)).insert("INSERT INTO tbl(name) VALUES(?)", VOID, "abc");
      QueryRunner queryRunner2 = QueryRunner.create(c2);
      long count = queryRunner2.select("SELECT COUNT(name) FROM unit_test.tbl", single(firstColumn()));
      assertEquals(0, count);
      
      c1.commit();
      
      count = queryRunner2.select("SELECT COUNT(name) FROM unit_test.tbl", single(firstColumn()));
      assertEquals(1, count);
    }
  }
  
  @Test
  public void should_run_in_transaction() throws Exception {
    queryRunner.tx((qr, tx) -> {
      qr.execute("INSERT INTO tbl(id, name) VALUES(?,?)", 1L, "a");
      tx.rollback();
      qr.execute("INSERT INTO tbl(id, name) VALUES(?,?)", 1L, "b");
      tx.commit();
    });
    
    String name = queryRunner.select("SELECT name FROM tbl WHERE id = ?", single(firstColumn()), 1L);
    
    assertEquals("b", name);
  }
  
  @Test
  public void should_implicitly_commit_transaction() throws Exception {
    queryRunner.tx((qr, tx) -> {
      qr.execute("INSERT INTO tbl(id, name) VALUES(?,?)", 1L, "a");
      tx.rollback();
      qr.execute("INSERT INTO tbl(id, name) VALUES(?,?)", 1L, "b");
    });
    
    String name = queryRunner.select("SELECT name FROM tbl WHERE id = ?", single(firstColumn()), 1L);
    
    assertEquals("b", name);
  }
  
  @Test
  public void should_not_implicitly_commit_transaction_if_auto_commit_already_set_to_false() throws Exception {
    String url = "jdbc:h2:mem:no_implicit_auto_commit";
    try (Connection c1 = DriverManager.getConnection(url); Connection c2 = DriverManager.getConnection(url)) {
      QueryRunner queryRunner1 = prepare(QueryRunner.create(c1));
      queryRunner1.initializeWith(c -> c.setAutoCommit(false))
        .tx((qr, tx) -> {
          qr.execute("INSERT INTO tbl(id, name) VALUES(?,?)", 1L, "a");
        }
      );
      
      QueryRunner queryRunner2 = QueryRunner.create(c2);
      queryRunner2.execute("SET SCHEMA unit_test");
      long count = queryRunner2.select("SELECT COUNT(id) FROM tbl WHERE id = ?", single(firstColumn()), 1L);
      
      assertEquals(0L, count);
      
      queryRunner1.tx((qr, tx) -> {
        tx.commit();
      });
      
      count = queryRunner2.select("SELECT COUNT(id) FROM tbl WHERE id = ?", single(firstColumn()), 1L);
      assertEquals(1L, count);
      assertFalse(c1.getAutoCommit());
    }
  }
  
  @Test
  public void should_map_column_with_valueOf() throws Exception {
    queryRunner.execute("INSERT INTO tbl(id, name) VALUES(?,?)", 1L, "a");
    
    ValueOf valueOf = queryRunner.select("SELECT name FROM tbl WHERE id = ?", single(firstColumn(ValueOf.class)), 1L);
    
    assertEquals("a", valueOf.value);
  }
  
  @Test
  public void should_set_object_field_to_null_if_not_found_in_result_set() throws Exception {
    queryRunner.execute("INSERT INTO tbl(id, name) VALUES(?,?)", 1L, "a");
    
    ValueOf valueOf = queryRunner.select("SELECT name FROM tbl WHERE id = ?", single(new ObjectRowProcessor<ValueOf>(ValueOf.class, (rs, i, o) -> { return Optional.empty(); })), 1L);
    
    assertNull(valueOf.value);
  }
  
  @Test
  public void should_select_with_null_param() throws Exception {
    queryRunner.batch("INSERT INTO tbl(name, amount) VALUES(?,?)", asList(asList("a", 1L), asList("b", null), asList(null, 3L)));
    
    Object nullObject = null;
    List<String> nullAmount = queryRunner.select("SELECT name FROM tbl WHERE amount IS ?", list(firstColumn()), nullObject);
    List<BigDecimal> nullName = queryRunner.select("SELECT amount FROM tbl WHERE name IS NOT DISTINCT FROM ?", list(firstColumn()), nullObject);
    List<String> eitherNull = queryRunner.select("SELECT CONCAT(IFNULL(tbl.name, 'null'), '-', IFNULL(amount, 'null')) FROM tbl WHERE name IS ? OR amount IS ?", list(firstColumn()), nullObject, nullObject);
    
    assertThat(nullAmount, contains("b"));
    assertThat(nullName, contains(BigDecimal.valueOf(300, 2)));
    assertThat(eitherNull, contains("b-null", "null-3.00"));
  }
  
  private static class Tbl {
    private Long id;
    private String name;
    private Instant instant;
    private Boolean active;
    private BigDecimal amount;
    private int num;
    private boolean active2;
  }
  
  private static class TblUnderscore {
    private Long idTbl;
    private String nameOf;
    private Instant instantAt;
    private Boolean isActive;
    private BigDecimal amountOwed;
    private int numOwned;
  }
  
  private static class ValueOf {
    private String value;
    
    @SuppressWarnings("unused")
    public static ValueOf valueOf(String value) {
      ValueOf valueOf = new ValueOf();
      valueOf.value = value;
      return valueOf;
    }
  }
  
  private QueryRunner prepare(QueryRunner qr) throws Exception {
    qr.execute("CREATE SCHEMA unit_test");
    qr.execute("SET SCHEMA unit_test");
    qr.execute("CREATE TABLE tbl (id IDENTITY PRIMARY KEY, name VARCHAR(255), instant TIMESTAMP, active BOOLEAN, amount DECIMAL(5,2), num INT)");
    qr.execute("CREATE TABLE tbl_underscore (id_tbl BIGINT AUTO_INCREMENT PRIMARY KEY, name_of VARCHAR(255), instant_at TIMESTAMP, is_active BOOLEAN, amount_owed DECIMAL(5,2), num_owned INT)");
    
    return qr;
  }
}
