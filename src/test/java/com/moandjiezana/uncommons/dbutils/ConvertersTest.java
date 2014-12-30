package com.moandjiezana.uncommons.dbutils;

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
}
