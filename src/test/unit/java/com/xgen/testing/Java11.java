package com.xgen.testing;

import org.junit.Assert;
import org.junit.Test;

public class Java11 {

  @Test
  public void testJava11Libraries() {
    // isBlank is a feature of String that was added in Java11
    Assert.assertTrue("".isBlank());
  }

  @FunctionalInterface
  private interface BiConsumerProducer<T, R> {
    R apply(T first, T second);
  }

  @Test
  public void testJava11Language() {
    // https://openjdk.java.net/jeps/323 was the only language addition to Java 11.
    BiConsumerProducer<String, String> concat = (var x, var y) -> x.concat(y);
    Assert.assertEquals("aa", concat.apply("a", "a"));
  }
}
