package com.xgen.mongot.index.path.string;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import org.junit.Assert;
import org.junit.Test;

public class UnresolvedStringFieldPathTest {
  @Test
  public void testGetValue() {
    FieldPath path = FieldPath.parse("foo");
    Assert.assertSame(path, new UnresolvedStringFieldPath(path).getValue());
  }

  @Test
  public void testEquality() {
    TestUtils.assertEqualityGroups(
        () -> UnresolvedStringPathBuilder.fieldPath("foo"),
        () -> UnresolvedStringPathBuilder.fieldPath("bar"),
        () -> FieldPath.parse("foo"),
        () -> new String("foo"));
  }
}
