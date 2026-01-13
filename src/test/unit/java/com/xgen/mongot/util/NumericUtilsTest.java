package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class NumericUtilsTest {

  @Test
  public void suspiciousBehavior() {
    // These test cover existing semantics even though they may not be intentional or MQL-compliant
    assertEquals(0, NumericUtils.compareDoubleLong(Double.NaN, Long.MAX_VALUE));
    assertEquals(0, NumericUtils.compareLongDouble(Long.MAX_VALUE, Double.NaN));
    assertEquals(1, NumericUtils.compareDoubleDouble(Double.NaN, Double.POSITIVE_INFINITY));

    // MQL treats -0.0 = 0.0 in sort, eq, and range queries.
    assertEquals(-1, NumericUtils.compareDoubleDouble(-0.0, 0.0));
    assertEquals(1, NumericUtils.compareDoubleDouble(0.0, -0.0));

    // Loss of precision is expected in current implementation, but not in MQL
    assertEquals(0, NumericUtils.compareLongDouble(1L << 61, (1L << 61) + 1.0));
    assertEquals(0, NumericUtils.compareDoubleLong((1L << 61) + 1.0, 1L << 61));
  }

  @Test
  public void zeros() {
    assertEquals(0, NumericUtils.compareLongDouble(0, 0.0));
    assertEquals(0, NumericUtils.compareDoubleLong(0.0, 0));
    assertEquals(0, NumericUtils.compareLongDouble(0L, -0.0));
    assertEquals(0, NumericUtils.compareDoubleLong(-0.0, 0L));
  }

  @Test
  public void nan() {
    assertEquals(0, NumericUtils.compareDoubleDouble(Double.NaN, Double.NaN));
    assertEquals(0, NumericUtils.compareLongDouble(0, -0.0));
    assertEquals(0, NumericUtils.compareLongDouble(0, 0.0));
  }

  @Test
  public void nonStandardNaN() {
    double nonstandardNaN = Double.longBitsToDouble(0x7FF8000000123456L);
    assertTrue(Double.isNaN(nonstandardNaN));
    assertNotEquals(
        Double.doubleToRawLongBits(Double.NaN), Double.doubleToRawLongBits(nonstandardNaN));

    assertEquals(0, NumericUtils.compareDoubleDouble(nonstandardNaN, Double.NaN));
  }

  @Test
  public void longDouble() {
    assertEquals(0, NumericUtils.compareLongDouble(0L, 0.0));
    assertEquals(1, NumericUtils.compareLongDouble(1L, 0.9));
    assertEquals(-1, NumericUtils.compareLongDouble(1L, 1.1));
    assertEquals(-1, NumericUtils.compareLongDouble(0L, Double.MIN_VALUE));
    assertEquals(1, NumericUtils.compareLongDouble(0L, -Double.MIN_VALUE));
    assertEquals(-1, NumericUtils.compareLongDouble(Long.MAX_VALUE, Double.POSITIVE_INFINITY));
    assertEquals(-1, NumericUtils.compareLongDouble(Long.MAX_VALUE, Double.MAX_VALUE));
  }

  @Test
  public void doubleLong() {
    assertEquals(0, NumericUtils.compareDoubleLong(0.0, 0L));
    assertEquals(-1, NumericUtils.compareDoubleLong(0.9, 1L));
    assertEquals(1, NumericUtils.compareDoubleLong(1.1, 1L));
    assertEquals(1, NumericUtils.compareDoubleLong(Double.MIN_VALUE, 0L));
    assertEquals(-1, NumericUtils.compareDoubleLong(-Double.MIN_VALUE, 0L));
    assertEquals(1, NumericUtils.compareDoubleLong(Double.POSITIVE_INFINITY, Long.MAX_VALUE));
    assertEquals(1, NumericUtils.compareDoubleLong(Double.MAX_VALUE, Long.MAX_VALUE));
  }
}
