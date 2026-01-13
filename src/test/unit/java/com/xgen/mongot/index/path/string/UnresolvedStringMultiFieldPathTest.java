package com.xgen.mongot.index.path.string;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import org.junit.Assert;
import org.junit.Test;

public class UnresolvedStringMultiFieldPathTest {
  @Test
  public void testFieldPath() {
    FieldPath path = FieldPath.parse("foo");
    UnresolvedStringMultiFieldPath multiPath = new UnresolvedStringMultiFieldPath(path, "bar");
    Assert.assertEquals(path, multiPath.getFieldPath());
  }

  @Test
  public void testMulti() {
    FieldPath path = FieldPath.parse("foo");
    UnresolvedStringMultiFieldPath multiPath = new UnresolvedStringMultiFieldPath(path, "bar");
    Assert.assertEquals("bar", multiPath.getMulti());
  }

  @Test
  public void testEquality() {
    TestUtils.assertEqualityGroups(
        () -> UnresolvedStringPathBuilder.withMulti("foo", "bar"),
        () -> UnresolvedStringPathBuilder.withMulti("bar", "foo"),
        () -> FieldPath.parse("foo"),
        () -> new String("foo"));
  }
}
