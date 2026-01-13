package com.xgen.mongot.index.lucene.query.sort;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.util.BsonUtils;
import java.util.UUID;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class SortUtilTest {

  @DataPoints
  public static final NullEmptySortPosition[] NULL_EMPTY_SORT_POSITIONS =
      NullEmptySortPosition.values();

  private static final BsonDouble NAN = new BsonDouble(Double.NaN);

  private static void assertCompare(
      int expected, BsonValue left, BsonValue right, NullEmptySortPosition nullEmptySortPosition) {

    assertEquals(expected, SortUtil.mqlMixedCompare(left, right, nullEmptySortPosition));
    assertEquals(-expected, SortUtil.mqlMixedCompare(right, left, nullEmptySortPosition));
  }

  @Ignore("Long-term fix requires Server support for noData")
  public void noDataStrictlyLessThanMinKey() {
    // Fixing this test case is prone to infinite GetMores (CLOUDP-259581)
    assertCompare(-1, BsonNull.VALUE, BsonUtils.MIN_KEY, NullEmptySortPosition.LOWEST);
  }

  @Ignore("Long-term fix requires Server support for noData")
  public void noDataStrictlyLargerThanMaxKey() {
    // Fixing this test case is prone to infinite GetMores (CLOUDP-259581)
    assertCompare(1, BsonNull.VALUE, BsonUtils.MAX_KEY, NullEmptySortPosition.HIGHEST);
  }

  @Test
  public void compareNullWhenNullEmptySortPositionLowest() {
    assertCompare(
        -1, BsonNull.VALUE, new BsonDouble(Double.NEGATIVE_INFINITY), NullEmptySortPosition.LOWEST);
    assertCompare(-1, BsonNull.VALUE, new BsonInt64(Long.MIN_VALUE), NullEmptySortPosition.LOWEST);
    assertCompare(-1, BsonNull.VALUE, new BsonString(""), NullEmptySortPosition.LOWEST);
    assertCompare(-1, BsonNull.VALUE, new BsonDateTime(1), NullEmptySortPosition.LOWEST);
    assertCompare(-1, BsonNull.VALUE, BsonUtils.MAX_KEY, NullEmptySortPosition.LOWEST);
  }

  @Test
  public void compareNullWhenNullEmptySortPositionHighest() {
    assertCompare(1, BsonNull.VALUE, BsonUtils.MIN_KEY, NullEmptySortPosition.HIGHEST);
    assertCompare(
        1, BsonNull.VALUE, new BsonDouble(Double.NEGATIVE_INFINITY), NullEmptySortPosition.HIGHEST);
    assertCompare(1, BsonNull.VALUE, new BsonInt64(Long.MIN_VALUE), NullEmptySortPosition.HIGHEST);
    assertCompare(1, BsonNull.VALUE, new BsonString(""), NullEmptySortPosition.HIGHEST);
    assertCompare(1, BsonNull.VALUE, new BsonDateTime(1), NullEmptySortPosition.HIGHEST);
  }

  @Test
  public void nullPriorityMatchesSentinels() {
    // Must compare equal as a quick fix for (CLOUDP-259581)
    assertEquals(
        SortUtil.getBracketPriority(BsonType.MIN_KEY, NullEmptySortPosition.LOWEST),
        SortUtil.getBracketPriority(BsonType.NULL, NullEmptySortPosition.LOWEST));

    assertEquals(
        SortUtil.getBracketPriority(BsonType.MAX_KEY, NullEmptySortPosition.HIGHEST),
        SortUtil.getBracketPriority(BsonType.NULL, NullEmptySortPosition.HIGHEST));
  }

  @Theory
  public void compareNan(
      @FromDataPoints("NULL_EMPTY_SORT_POSITIONS") NullEmptySortPosition nullEmptySortPosition) {
    assertCompare(1, NAN, BsonUtils.MIN_KEY, nullEmptySortPosition);
    assertCompare(1, NAN, BsonNull.VALUE, NullEmptySortPosition.LOWEST);
    assertCompare(-1, NAN, BsonNull.VALUE, NullEmptySortPosition.HIGHEST);
    assertCompare(-1, NAN, BsonUtils.MAX_KEY, nullEmptySortPosition);
    assertCompare(-1, NAN, new BsonDouble(Double.NEGATIVE_INFINITY), nullEmptySortPosition);
    assertCompare(-1, NAN, new BsonInt64(Long.MIN_VALUE), nullEmptySortPosition);
  }

  @Theory
  public void compareSelf(
      @FromDataPoints("NULL_EMPTY_SORT_POSITIONS") NullEmptySortPosition nullEmptySortPosition) {
    assertCompare(0, BsonNull.VALUE, BsonNull.VALUE, nullEmptySortPosition);
    assertCompare(0, NAN, NAN, nullEmptySortPosition);
    assertCompare(0, new BsonString(""), new BsonString(""), nullEmptySortPosition);
    assertCompare(0, new BsonDateTime(0), new BsonDateTime(0), nullEmptySortPosition);
    assertCompare(0, BsonUtils.MAX_KEY, BsonUtils.MAX_KEY, nullEmptySortPosition);
    assertCompare(0, BsonUtils.MIN_KEY, BsonUtils.MIN_KEY, nullEmptySortPosition);
  }

  @Theory
  public void compareMixedNumbers(
      @FromDataPoints("NULL_EMPTY_SORT_POSITIONS") NullEmptySortPosition nullEmptySortPosition) {
    assertCompare(-1, new BsonDouble(0), new BsonInt64(1), nullEmptySortPosition);
    assertCompare(0, new BsonDouble(0), new BsonInt64(0), nullEmptySortPosition);
    assertCompare(1, new BsonDouble(0), new BsonInt64(-1), nullEmptySortPosition);
    assertCompare(
        -1,
        new BsonDouble(Double.NEGATIVE_INFINITY),
        new BsonInt64(Long.MIN_VALUE),
        nullEmptySortPosition);

    double maxPreciseLong = Math.pow(2, 53);
    assertCompare(
        0,
        new BsonInt64((long) maxPreciseLong),
        new BsonDouble(maxPreciseLong),
        nullEmptySortPosition);
  }

  @Theory
  public void compareMixedBrackets(
      @FromDataPoints("NULL_EMPTY_SORT_POSITIONS") NullEmptySortPosition nullEmptySortPosition) {
    assertCompare(-1, new BsonDouble(0), new BsonString(""), nullEmptySortPosition);
    assertCompare(-1, new BsonDouble(0), new BsonDateTime(0), nullEmptySortPosition);
    assertCompare(-1, new BsonString(""), new BsonDateTime(0), nullEmptySortPosition);
  }

  @Theory
  public void compareSameType(
      @FromDataPoints("NULL_EMPTY_SORT_POSITIONS") NullEmptySortPosition nullEmptySortPosition) {
    assertCompare(-1, new BsonDouble(1), new BsonDouble(2), nullEmptySortPosition);
    assertCompare(-1, new BsonInt64(0), new BsonInt64(1), nullEmptySortPosition);
    assertCompare(-1, new BsonString("1"), new BsonString("2"), nullEmptySortPosition);
    assertCompare(-1, new BsonDateTime(0), new BsonDateTime(1), nullEmptySortPosition);
    assertCompare(-1, new BsonInt64(1L << 54), new BsonInt64(1L << 54 + 1), nullEmptySortPosition);
  }

  @Test
  public void stringConversionIsLossless() {
    String original = "わかりません";
    BytesRef utf8 = new BytesRef(original);

    String recovered = BsonUtils.STRING_CONVERTER.apply(utf8).asString().getValue();

    assertEquals(original, recovered);
  }

  @Test
  public void uuidBsonBinary() {
    byte[] smallUuidBytes =
        new byte[] {
          0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11,
          0x00
        };
    byte[] binary =
        new BsonBinary(UUID.fromString("01010000-0000-0000-0000-000000001100"))
            .asBinary()
            .getData();
    assertArrayEquals(binary, smallUuidBytes);
  }
}
