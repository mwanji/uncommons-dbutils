package com.moandjiezana.uncommons.dbutils;

import java.util.ArrayList;
import java.util.List;

public class Joined {
  Tbl tbl;
  TblUnderscore tblUnderscore;
  Long id;
  String name;
  List<Joined.Relation> relations = new ArrayList<>();
  
  public static class Relation {
    Long id;
    String name;
  }
}