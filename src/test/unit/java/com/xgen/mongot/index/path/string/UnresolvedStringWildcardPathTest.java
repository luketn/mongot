package com.xgen.mongot.index.path.string;

import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import org.junit.Assert;
import org.junit.Test;

public class UnresolvedStringWildcardPathTest {
  @Test
  public void testGetValue() {
    Assert.assertSame("foo*", UnresolvedStringPathBuilder.wildcardPath("foo*").getValue());
  }

  @Test
  public void testEquality() {
    TestUtils.assertEqualityGroups(
        () -> UnresolvedStringPathBuilder.wildcardPath("foo*"),
        () -> UnresolvedStringPathBuilder.wildcardPath("bar*"));
  }
}
