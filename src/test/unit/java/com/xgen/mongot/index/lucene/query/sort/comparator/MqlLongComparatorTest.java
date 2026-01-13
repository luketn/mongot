package com.xgen.mongot.index.lucene.query.sort.comparator;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.testing.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class MqlLongComparatorTest {
  @DataPoints
  public static final Long[] longValues = {Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE, null};

  @DataPoints public static final double[] doubleValues = TestUtils.createWeirdDoubles();

  @DataPoints
  public static final NullEmptySortPosition[] nullEmptySortPositions =
      NullEmptySortPosition.values();

  @Theory
  public void testMqlLongCompareNull(Long first, Long second) {
    Assume.assumeTrue(first == null || second == null);

    if (first == null && second == null) {
      Assert.assertEquals(
          0, MqlLongComparator.mqlLongCompare(first, second, NullEmptySortPosition.LOWEST));
    } else if (first == null) {
      Assert.assertTrue(
          MqlLongComparator.mqlLongCompare(first, second, NullEmptySortPosition.LOWEST) < 0);
      Assert.assertTrue(
          MqlLongComparator.mqlLongCompare(first, second, NullEmptySortPosition.HIGHEST) > 0);
    } else {
      Assert.assertTrue(
          MqlLongComparator.mqlLongCompare(first, second, NullEmptySortPosition.LOWEST) > 0);
      Assert.assertTrue(
          MqlLongComparator.mqlLongCompare(first, second, NullEmptySortPosition.HIGHEST) < 0);
    }
  }

  @Theory
  public void testMqlLongCompareLongs(
      Long first, Long second, NullEmptySortPosition nullEmptySortPosition) {
    Assume.assumeTrue(first != null && second != null);

    Assert.assertEquals(
        "Remaining values must compare same as Long.compare()",
        Long.compare(first, second),
        MqlLongComparator.mqlLongCompare(first, second, nullEmptySortPosition));
  }

  @Theory
  public void mqlLongCompare_encodedNaN_isMinimal(
      double first, double second, NullEmptySortPosition nullEmptySortPosition) {
    Assume.assumeTrue(Double.isNaN(first) || Double.isNaN(second));

    long firstLong = LuceneDoubleConversionUtils.toMqlSortableLong(first);
    long secondLong = LuceneDoubleConversionUtils.toMqlSortableLong(second);
    int result = MqlLongComparator.mqlLongCompare(firstLong, secondLong, nullEmptySortPosition);

    if (Double.isNaN(first) && Double.isNaN(second)) {
      assertEquals(0, result);
    } else if (Double.isNaN(first)) {
      assertEquals(-1, result);
    } else {
      assertEquals(1, result);
    }
  }

  @Theory
  public void mqlLongCompare_encodedNormals_matchDoubleCompare(
      double first, double second, NullEmptySortPosition nullEmptySortPosition) {
    Assume.assumeTrue(!Double.isNaN(first) && !Double.isNaN(second));

    long firstLong = LuceneDoubleConversionUtils.toMqlSortableLong(first);
    long secondLong = LuceneDoubleConversionUtils.toMqlSortableLong(second);
    int result = MqlLongComparator.mqlLongCompare(firstLong, secondLong, nullEmptySortPosition);

    assertEquals(
        "Remaining values must compare same as Double.compare()",
        Double.compare(first, second),
        result);
  }
}
