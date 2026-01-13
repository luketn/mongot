package com.xgen.mongot.index.lucene.query.pushdown;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;

public class MqlComparatorTest {

  /** Returns sign of argument. */
  private static int signum(int value) {
    return Integer.compare(value, 0);
  }

  private static void assertCmp(int expected, BsonValue left, BsonValue right) {
    assertCmp(expected, left, right, ArrayComparator.MIN);
  }

  private static void assertCmp(
      int expected, BsonValue left, BsonValue right, ArrayComparator cmp) {
    assertEquals(expected, signum(MqlComparator.compareValues(left, right, cmp)));
    assertEquals(-expected, signum(MqlComparator.compareValues(right, left, cmp)));
  }

  @Test
  public void compareValues() {
    assertCmp(0, new BsonString("foo"), new BsonSymbol("foo"));
    assertCmp(1, new BsonString("food"), new BsonSymbol("foo"));
  }

  @Test
  public void compareBinaryShorterValuesComeFirst() {
    byte[] longData = new byte[10];
    byte[] shortData = new byte[8];
    Arrays.fill(shortData, (byte) 1);
    assertCmp(
        1,
        new BsonBinary(BsonBinarySubType.BINARY, longData),
        new BsonBinary(BsonBinarySubType.BINARY, shortData));
  }

  @Test
  public void compareBinaryTieBreakLengthByUnsignedSubtype() {
    byte[] zeros = new byte[8];
    byte[] ones = new byte[8];
    Arrays.fill(ones, (byte) 1);

    assertCmp(-1, new BsonBinary((byte) 0, zeros), new BsonBinary((byte) 1, ones));
    assertCmp(-1, new BsonBinary((byte) 0, ones), new BsonBinary((byte) 1, zeros));

    assertCmp(-1, new BsonBinary((byte) 0, zeros), new BsonBinary((byte) -1, ones));
    assertCmp(-1, new BsonBinary((byte) 0, ones), new BsonBinary((byte) -1, zeros));
  }

  @Test
  public void compareBinaryCompareDataUnsigned() {
    byte[] zeros = new byte[8];
    byte[] maxPositive = new byte[8];
    byte[] negative = new byte[8];
    Arrays.fill(maxPositive, Byte.MAX_VALUE);
    Arrays.fill(negative, Byte.MIN_VALUE);

    assertCmp(
        -1,
        new BsonBinary(BsonBinarySubType.BINARY, zeros),
        new BsonBinary(BsonBinarySubType.BINARY, maxPositive));
    assertCmp(
        -1,
        new BsonBinary(BsonBinarySubType.BINARY, maxPositive),
        new BsonBinary(BsonBinarySubType.BINARY, negative));
    assertCmp(
        -1,
        new BsonBinary(BsonBinarySubType.BINARY, zeros),
        new BsonBinary(BsonBinarySubType.BINARY, negative));
  }

  @Test
  public void compareDocument() {
    assertCmp(0, new BsonDocument(), new BsonDocument());
    assertCmp(-1, new BsonDocument(), new BsonDocument("foo", BsonNull.VALUE));
    assertCmp(1, new BsonDocument("foo", BsonNull.VALUE), new BsonDocument());
    assertCmp(0, new BsonDocument("foo", BsonNull.VALUE), new BsonDocument("foo", BsonNull.VALUE));
    assertCmp(1, new BsonDocument("foo", BsonNull.VALUE), new BsonDocument("a", BsonNull.VALUE));
    assertCmp(
        -1, new BsonDocument("foo", BsonNull.VALUE), new BsonDocument("foo", new BsonInt64(1)));
    assertCmp(
        1, new BsonDocument("foo", new BsonInt64(2)), new BsonDocument("foo", new BsonInt64(1)));
  }

  @Test
  public void compareArray() {
    assertCmp(
        1,
        new BsonArray(List.of(new BsonInt64(2), new BsonInt64(1))),
        new BsonArray(List.of(new BsonInt64(1), new BsonInt64(2))),
        ArrayComparator.LEXICOGRAPHIC);

    assertCmp(
        -1,
        new BsonArray(List.of(new BsonInt64(0), new BsonInt64(3))),
        new BsonArray(List.of(new BsonInt64(1), new BsonInt64(2))),
        ArrayComparator.MIN);

    assertCmp(
        1,
        new BsonArray(List.of(new BsonInt64(0), new BsonInt64(3))),
        new BsonArray(List.of(new BsonInt64(1), new BsonInt64(2))),
        ArrayComparator.MAX);
  }

  @Test
  public void decimal128AndDouble() {
    assertCmp(0, new BsonDecimal128(Decimal128.NaN), new BsonDouble(Double.NaN));
    assertCmp(-1, new BsonDecimal128(Decimal128.NaN), new BsonDouble(0.0));
    assertCmp(1, new BsonDecimal128(Decimal128.POSITIVE_ZERO), new BsonDouble(Double.NaN));

    assertCmp(0, new BsonDecimal128(Decimal128.NEGATIVE_ZERO), new BsonDouble(0.0));
    assertCmp(0, new BsonDecimal128(Decimal128.POSITIVE_ZERO), new BsonDouble(-0.0));

    assertCmp(1, new BsonDecimal128(Decimal128.parse("5")), new BsonDouble(-5));
    assertCmp(0, new BsonDecimal128(Decimal128.parse("5")), new BsonDouble(5));
  }

  @Test
  public void decimal128() {
    assertCmp(1, new BsonInt32(0), new BsonDecimal128(Decimal128.NaN));
    assertCmp(1, new BsonInt64(0), new BsonDecimal128(Decimal128.NaN));
    assertCmp(1, new BsonDouble(0), new BsonDecimal128(Decimal128.NaN));
    assertCmp(0, new BsonDecimal128(Decimal128.NaN), new BsonDecimal128(Decimal128.NaN));
  }

  @Test
  public void dbRef() {
    var f = new ObjectId("F".repeat(24));
    var a = new ObjectId("A".repeat(24));
    assertCmp(0, new BsonDbPointer("namespace", f), new BsonDbPointer("namespace", f));
    assertCmp(-1, new BsonDbPointer("", f), new BsonDbPointer("namespace", a));
    assertCmp(-1, new BsonDbPointer("namespace", a), new BsonDbPointer("namespace", f));
    assertCmp(1, new BsonDbPointer("aaaaaaaaaaaa", a), new BsonDbPointer("namespace", f));
    assertCmp(1, new BsonDbPointer("ñåµéspåçé", a), new BsonDbPointer("namespace", f));
  }
}
