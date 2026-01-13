package com.xgen.mongot.index.path.string;

import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import org.junit.Assert;
import org.junit.Test;

public class StringMultiFieldPathTest {

  @Test
  public void testGetType() {
    Assert.assertEquals(
        StringPath.Type.MULTI_FIELD, StringPathBuilder.withMulti("foo", "bar").getType());
  }

  @Test
  public void testFieldPath() {
    FieldPath path = FieldPath.parse("foo");
    StringMultiFieldPath multiPath = new StringMultiFieldPath(path, "bar");
    Assert.assertEquals(path, multiPath.getFieldPath());
  }

  @Test
  public void testMulti() {
    FieldPath path = FieldPath.parse("foo");
    StringMultiFieldPath multiPath = new StringMultiFieldPath(path, "bar");
    Assert.assertEquals("bar", multiPath.getMulti());
  }

  @Test
  public void testEquality() {
    TestUtils.assertEqualityGroups(
        () -> StringPathBuilder.withMulti("foo", "bar"),
        () -> StringPathBuilder.withMulti("bar", "foo"),
        () -> FieldPath.parse("foo"),
        () -> new String("foo"));
  }
}
