package com.moandjiezana.uncommons.dbutils;

public class ValueOf {
  String value;
  
  public static ValueOf valueOf(String value) {
    ValueOf valueOf = new ValueOf();
    valueOf.value = value;
    return valueOf;
  }
}