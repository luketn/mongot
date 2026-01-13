package com.xgen.mongot.index.path.string;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import org.junit.Assert;
import org.junit.Test;

public class StringFieldPathTest {

  @Test
  public void testGetType() {
    Assert.assertEquals(StringPath.Type.FIELD, StringPathBuilder.fieldPath("foo").getType());
  }

  @Test
  public void testGetValue() {
    FieldPath path = FieldPath.parse("foo");
    Assert.assertSame(path, new StringFieldPath(path).getValue());
  }

  @Test
  public void testEquality() {
    TestUtils.assertEqualityGroups(
        () -> StringPathBuilder.fieldPath("foo"),
        () -> StringPathBuilder.fieldPath("bar"),
        () -> FieldPath.parse("foo"),
        () -> new String("foo"));
  }
}
