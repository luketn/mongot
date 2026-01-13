package com.xgen.mongot.index.query.sort;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.util.BsonUtils;
import org.junit.Test;

public class NullEmptySortPositionTest {
  @Test
  public void testNullMissingPriority() {
    assertEquals(10, NullEmptySortPosition.LOWEST.nullMissingPriority);
    assertEquals(240, NullEmptySortPosition.HIGHEST.nullMissingPriority);
  }

  @Test
  public void testEmptyArrayPriority() {
    assertEquals(10, NullEmptySortPosition.LOWEST.emptyArrayPriority);
    assertEquals(240, NullEmptySortPosition.HIGHEST.emptyArrayPriority);
  }

  @Test
  public void testNullMissingSortValue() {
    assertEquals(BsonUtils.MIN_KEY, NullEmptySortPosition.LOWEST.getNullMissingSortValue());
    assertEquals(BsonUtils.MAX_KEY, NullEmptySortPosition.HIGHEST.getNullMissingSortValue());
  }
}
