package com.xgen.mongot.index.lucene.query.pushdown;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.BsonUtils;
import java.util.Comparator;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonUndefined;
import org.junit.Test;

public class ArrayComparatorTest {

  /** Returns sign of argument. */
  private static int signum(int v) {
    return Integer.compare(v, 0);
  }

  private static <T> void assertCmp(int expected, Comparator<T> cmp, T left, T right) {
    assertEquals(expected, signum(cmp.compare(left, right)));
    assertEquals(-expected, signum(cmp.compare(right, left)));
  }

  @Test
  public void lexicographic() {
    assertCmp(0, ArrayComparator.LEXICOGRAPHIC, new BsonArray(), new BsonArray());
    assertCmp(
        -1,
        ArrayComparator.LEXICOGRAPHIC,
        new BsonArray(),
        new BsonArray(List.of(BsonUtils.MIN_KEY)));
    assertCmp(
        1,
        ArrayComparator.LEXICOGRAPHIC,
        new BsonArray(List.of(new BsonInt64(0), new BsonInt64(2))),
        new BsonArray(List.of(new BsonInt64(0), new BsonInt64(1))));
    assertCmp(
        1,
        ArrayComparator.LEXICOGRAPHIC,
        new BsonArray(List.of(new BsonArray(List.of(new BsonInt64(0), new BsonInt64(2))))),
        new BsonArray(List.of(new BsonArray(List.of(new BsonInt64(0), new BsonInt64(1))))));
  }

  @Test
  public void min() {
    assertCmp(0, ArrayComparator.MIN, new BsonArray(), new BsonArray());
    assertCmp(1, ArrayComparator.MIN, new BsonArray(), new BsonArray(List.of(BsonUtils.MIN_KEY)));
    assertCmp(0, ArrayComparator.MIN, new BsonArray(), new BsonArray(List.of(new BsonUndefined())));
    assertCmp(-1, ArrayComparator.MIN, new BsonArray(), new BsonArray(List.of(new BsonNull())));
    assertCmp(
        1,
        ArrayComparator.MIN,
        new BsonArray(List.of(new BsonInt64(5), new BsonInt64(2))),
        new BsonArray(List.of(new BsonInt64(6), new BsonInt64(1))));
    assertCmp(
        1,
        ArrayComparator.MIN,
        new BsonArray(List.of(new BsonArray(List.of(new BsonInt64(5), new BsonInt64(2))))),
        new BsonArray(List.of(new BsonArray(List.of(new BsonInt64(6), new BsonInt64(1))))));
  }

  @Test
  public void max() {
    assertCmp(0, ArrayComparator.MAX, new BsonArray(), new BsonArray());
    assertCmp(1, ArrayComparator.MAX, new BsonArray(), new BsonArray(List.of(BsonUtils.MIN_KEY)));
    assertCmp(0, ArrayComparator.MAX, new BsonArray(), new BsonArray(List.of(new BsonUndefined())));
    assertCmp(-1, ArrayComparator.MAX, new BsonArray(), new BsonArray(List.of(new BsonNull())));
    assertCmp(
        -1,
        ArrayComparator.MAX,
        new BsonArray(List.of(new BsonInt64(5), new BsonInt64(2))),
        new BsonArray(List.of(new BsonInt64(6), new BsonInt64(1))));
    assertCmp(
        -1,
        ArrayComparator.MAX,
        new BsonArray(List.of(new BsonArray(List.of(new BsonInt64(5), new BsonInt64(2))))),
        new BsonArray(List.of(new BsonArray(List.of(new BsonInt64(6), new BsonInt64(1))))));
  }
}
