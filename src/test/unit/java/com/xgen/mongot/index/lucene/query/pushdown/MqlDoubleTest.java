package com.xgen.mongot.index.lucene.query.pushdown;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.junit.Test;

public class MqlDoubleTest {

  private static void assertCmp(int expected, double left, double right) {
    assertEquals(expected, MqlDouble.compare(left, right));
    assertEquals(-expected, MqlDouble.compare(right, left));

    assertEquals(
        expected,
        MqlComparator.compareValues(
            new BsonDouble(left), new BsonDouble(right), ArrayComparator.MIN));
  }

  private static void assertCmp(int expected, double left, long right) {
    if (Double.isFinite(left)) {
      assertEquals(expected, new BigDecimal(left).compareTo(new BigDecimal(right)));
    }
    assertEquals(expected, MqlDouble.compare(left, right));
    assertEquals(-expected, MqlDouble.compare(right, left));

    assertEquals(
        expected,
        MqlComparator.compareValues(
            new BsonDouble(left), new BsonInt64(right), ArrayComparator.MIN));
  }

  @Test
  public void compareDoubleDouble() {
    assertCmp(0, Math.PI, Math.PI);
    assertCmp(0, 0.0, -0.0);
    assertCmp(0, Double.NaN, Double.longBitsToDouble(0x7ff8000000000001L));
    assertCmp(-1, 1.0, 2.0);
    assertCmp(-1, Double.MAX_VALUE, Double.POSITIVE_INFINITY);
  }

  @Test
  public void compareDoubleLong() {
    long smallestLongThatRoundsDown = 9007199254740993L;
    long smallestLongThatRoundsUp = 9007199254740995L;

    assertCmp(-1, 1.0, 2);
    assertCmp(1, Math.PI, 3);
    assertCmp(0, -0.0, 0);
    assertCmp(-1, Double.NaN, Long.MIN_VALUE);
    assertCmp(-1, Double.NEGATIVE_INFINITY, Long.MIN_VALUE);
    assertCmp(1, Double.POSITIVE_INFINITY, Long.MAX_VALUE);
    assertCmp(1, Math.nextUp(1L << 53), 1L << 53);
    assertCmp(-1, Math.nextUp(1L << 53), 1L << 53 + 1L);
    assertCmp(-1, (double) smallestLongThatRoundsDown, smallestLongThatRoundsDown);
    assertCmp(1, (double) smallestLongThatRoundsUp, smallestLongThatRoundsUp);
  }
}
