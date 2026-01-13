package com.xgen.mongot.index.version;

import com.xgen.testing.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UserIndexVersionTest {

  @Test
  public void testUserIndexVersionEquals() {
    TestUtils.assertEqualityGroups(() -> new UserIndexVersion(1), () -> new UserIndexVersion(2));
  }

  @Test
  public void versionMustBeNonNegative() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new UserIndexVersion(-1));
    new UserIndexVersion(0);
  }
}
