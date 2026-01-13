package com.xgen.mongot.index.lucene.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.xgen.testing.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class LuceneDoubleConversionUtilsTest {

  @DataPoints
  public static double[] weirdDoubles = TestUtils.createWeirdDoubles();

  @Theory
  public void toMqlSortableLong_isBijective(double d) {
    long sortableLong = LuceneDoubleConversionUtils.toMqlSortableLong(d);
    double recovered = LuceneDoubleConversionUtils.fromMqlSortableLong(sortableLong);

    // Compare doubles with bitwise semantics, coalescing NaNs to same pattern
    assertEquals(Double.doubleToLongBits(d), Double.doubleToLongBits(recovered));
    assertThat(sortableLong).isNoneOf(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Test
  public void testToLongNearZero() {
    Assert.assertEquals("negative zero equals zero", 0L, LuceneDoubleConversionUtils.toLong(-0.0));
    Assert.assertEquals("positive zero equals zero", 0L, LuceneDoubleConversionUtils.toLong(0.0));
    Assert.assertEquals(
        "positive number", 1L, LuceneDoubleConversionUtils.toLong(Math.nextUp(0.0)));
    Assert.assertEquals(
        "negative number", -1L, LuceneDoubleConversionUtils.toLong(Math.nextDown(0.0)));
  }

  @Test
  public void testFromLongNearZero() {
    Assert.assertEquals("zero equals zero", 0.0, LuceneDoubleConversionUtils.fromLong(0), 0);
    Assert.assertEquals(
        "positive number", Math.nextUp(0.0), LuceneDoubleConversionUtils.fromLong(1L), 0);
    Assert.assertEquals(
        "negative number", Math.nextDown(0.0), LuceneDoubleConversionUtils.fromLong(-1L), 0);
  }

  @Test
  public void testToLongPositive() {
    Assert.assertEquals(
        "positive number", 20L, LuceneDoubleConversionUtils.toLong(0.0 + 20 * Math.ulp(0.0)));
    Assert.assertEquals(
        "larger positive number",
        123456L,
        LuceneDoubleConversionUtils.toLong(0.0 + 123456 * Math.ulp(0.0)));
  }

  @Test
  public void testFromLongPositive() {
    Assert.assertEquals(
        "positive number", 20 * Math.ulp(0.0), LuceneDoubleConversionUtils.fromLong(20L), 0);
    Assert.assertEquals(
        "greater number", 123456 * Math.ulp(0.0), LuceneDoubleConversionUtils.fromLong(123456L), 0);
  }

  @Test
  public void testToLongNegative() {
    Assert.assertEquals(
        "negative number", -20L, LuceneDoubleConversionUtils.toLong(-20 * Math.ulp(0.0)));
    Assert.assertEquals(
        "lesser negative number",
        -123456L,
        LuceneDoubleConversionUtils.toLong(-123456 * Math.ulp(0.0)));
  }

  @Test
  public void testFromLongNegative() {
    Assert.assertEquals(
        "negative number", -20 * Math.ulp(0.0), LuceneDoubleConversionUtils.fromLong(-20L), 0);
    Assert.assertEquals(
        "lesser negative number",
        -123456 * Math.ulp(0.0),
        LuceneDoubleConversionUtils.fromLong(-123456L),
        0);
  }

  @Test
  public void testToLongNearPrecisionLimits() {
    Assert.assertEquals(
        "near positive precision limit",
        9007199254740992L, // 2^53, the precision limit
        LuceneDoubleConversionUtils.toLong(9007199254740992L * Math.ulp(0.0)));
    Assert.assertEquals(
        "near negative precision limit",
        -9007199254740992L, // -2^53, the precision limit
        LuceneDoubleConversionUtils.toLong(-9007199254740992L * Math.ulp(0.0)));
    Assert.assertNotEquals(
        "above positive precision limit",
        9007199254740993L, // 2^53 + 1
        LuceneDoubleConversionUtils.toLong(9007199254740993L * Math.ulp(0.0)));
    Assert.assertNotEquals(
        "below negative precision limit",
        -9007199254740993L, // -2^53 - 1
        LuceneDoubleConversionUtils.toLong(-9007199254740993L * Math.ulp(0.0)));
  }

  @Test
  public void testFromLongNearPrecisionLimits() {
    Assert.assertEquals(
        "near positive precision limit",
        9007199254740992L * Math.ulp(0.0),
        LuceneDoubleConversionUtils.fromLong(9007199254740992L),
        0);
    Assert.assertEquals(
        "near negative precision limit",
        -9007199254740992L * Math.ulp(0.0),
        LuceneDoubleConversionUtils.fromLong(-9007199254740992L),
        0);
    Assert.assertNotEquals(
        "above positive precision limit",
        9007199254740993L * Math.ulp(0.0),
        LuceneDoubleConversionUtils.fromLong(9007199254740993L));
    Assert.assertNotEquals(
        "near negative precision limit",
        -9007199254740993L * Math.ulp(0.0),
        LuceneDoubleConversionUtils.fromLong(-9007199254740993L));
  }

  @Test
  public void roundTripTests() {
    // Negative zero does is collapsed to +0.0 in this conversion, so is not tested here.
    roundTripTest(0.0);

    roundTripTest(42.0);
    roundTripTest(-42.0);

    roundTripTest(Double.MAX_VALUE);
    roundTripTest(-1.0 * Double.MAX_VALUE);

    roundTripTest(Double.MIN_VALUE);
    roundTripTest(-1.0 * Double.MIN_VALUE);

    roundTripTest(9007199154730992L);
    roundTripTest(9007199154730993L);
  }

  private static void roundTripTest(double value) {
    double roundTrip =
        LuceneDoubleConversionUtils.fromLong(LuceneDoubleConversionUtils.toLong(value));
    Assert.assertEquals("round trip should result in exactly the same number", value, roundTrip, 0);
    Assert.assertEquals(
        "round trip should result in exactly the same number",
        Double.doubleToRawLongBits(value),
        Double.doubleToRawLongBits(roundTrip));
  }

  private static void roundTripTest(long value) {
    long roundTrip =
        (long) LuceneDoubleConversionUtils.fromLong(LuceneDoubleConversionUtils.toLong(value));
    Assert.assertEquals("round trip should result in exactly the same number", value, roundTrip);
  }
}
