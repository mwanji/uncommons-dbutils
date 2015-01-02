package com.moandjiezana.uncommons.dbutils;

import java.math.BigDecimal;
import java.time.Instant;

public class TblBean {
  private Long id;
  private String name;
  private Instant instant;
  private Boolean active;
  private BigDecimal amount;
  private int num;
  
  public Long getId() {
    return id;
  }
  public void setId(Long id) {
    this.id = id + 1;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name + "_property";
  }
  public Instant getInstant() {
    return instant;
  }
  public void setInstant(Instant instant) {
    this.instant = instant.plusSeconds(60);
  }
  public Boolean isActive() {
    return active;
  }
  public void setActive(Boolean active) {
    this.active = !active;
  }
  public BigDecimal getAmount() {
    return amount;
  }
  public void setAmount(BigDecimal amount) {
    this.amount = amount.add(BigDecimal.valueOf(1));
  }
  public int getNum() {
    return num;
  }
  public void setNum(int num) {
    this.num = num + 1;
  }
}
