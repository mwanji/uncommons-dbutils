package com.moandjiezana.uncommons.dbutils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class CaseInsensitiveMap<V> implements Map<String, V> {
  
  private final Map<String, V> map = new HashMap<>();

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key.toString().toLowerCase());
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return map.get(key.toString().toLowerCase());
  }

  @Override
  public V put(String key, V value) {
    return map.put(key.toString().toLowerCase(), value);
  }

  @Override
  public V remove(Object key) {
    return map.remove(key.toString().toLowerCase());
  }

  @Override
  public void putAll(Map<? extends String, ? extends V> m) {
    map.putAll(m.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString().toLowerCase(), Map.Entry::getValue)));
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<String> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<V> values() {
    return map.values();
  }

  @Override
  public Set<java.util.Map.Entry<String, V>> entrySet() {
    return map.entrySet();
  }
}