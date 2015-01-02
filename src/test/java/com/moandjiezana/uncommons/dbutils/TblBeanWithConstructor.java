package com.moandjiezana.uncommons.dbutils;

import java.beans.ConstructorProperties;
import java.math.BigDecimal;
import java.time.Instant;

public class TblBeanWithConstructor {
  private final Long id;
  private final String name;
  private Instant instant;
  private Boolean active;
  private BigDecimal amount;
  private int num;
  
  @ConstructorProperties({ "id", "name" })
  public TblBeanWithConstructor(Long id, String name) {
    this.id = id + 1;
    this.name = name + "_constructor";
  }
  
  public Long getId() {
    return id;
  }

  public String getName() {
    return name;
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
