package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FunctionalUtilsTest {

  @Test
  public void testGetOrDefaultIfThrows_shouldGetDefaultWhenThrows() {
    int result =
        FunctionalUtils.getOrDefaultIfThrows(
            () -> {
              throw new IllegalArgumentException("test");
            },
            IllegalArgumentException.class,
            94);
    assertEquals(94, result);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetOrDefaultIfThrows_shouldThrowOnUnexpectedException() {
    int result =
        FunctionalUtils.getOrDefaultIfThrows(
            () -> {
              throw new IllegalStateException("test");
            },
            IllegalArgumentException.class,
            94);
    assertEquals(94, result);
  }

  @Test
  public void testGetOrDefaultIfThrows_shouldReturnResultWhenNoExceptionIsThrown() {
    int result = FunctionalUtils.getOrDefaultIfThrows(() -> 1, IllegalArgumentException.class, 94);
    assertEquals(1, result);
  }
}
