package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class StreamsTest {

  @Test
  public void testEnumerate() {
    assertEquals(
        "enumeration did not match",
        List.of(
            new Streams.Enumeration<>(0, 1),
            new Streams.Enumeration<>(1, 2),
            new Streams.Enumeration<>(2, 3)),
        Streams.enumerate(List.of(1, 2, 3)).collect(Collectors.toList()));
  }

  @Test
  public void testEnumerateEmptyList() {
    assertEquals(
        "enumeration did not match",
        Collections.emptyList(),
        Streams.enumerate(Collections.emptyList()).collect(Collectors.toList()));
  }
}
