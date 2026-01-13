package com.xgen.mongot.index.version;

import com.xgen.testing.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class IndexFormatVersionTest {

  @Test
  public void testBackendIndexVersionEquals() {
    TestUtils.assertEqualityGroups(
        () -> IndexFormatVersion.create(1), () -> IndexFormatVersion.create(2));
  }

  @Test
  public void versionMustBePositive() {
    Assert.assertThrows(IllegalArgumentException.class, () -> IndexFormatVersion.create(0));
    Assert.assertThrows(IllegalArgumentException.class, () -> IndexFormatVersion.create(-1));
    IndexFormatVersion.create(1);
  }

  @Test
  public void testConstantVersionFields() {
    Assert.assertEquals(
        1,
        IndexFormatVersion.CURRENT.versionNumber
            - IndexFormatVersion.MIN_SUPPORTED_VERSION.versionNumber);
  }

  @Test
  public void testEquality() {
    Assert.assertEquals(IndexFormatVersion.FIVE, IndexFormatVersion.create(5));
    Assert.assertEquals(IndexFormatVersion.SIX, IndexFormatVersion.create(6));
  }

  @Test
  public void minVectorFeatureVersion_currentAndPreviousFormatVersions_equalsTo3() {
    Assert.assertEquals(3, IndexFormatVersion.FIVE.minVectorFeatureVersion());
    Assert.assertEquals(3, IndexFormatVersion.SIX.minVectorFeatureVersion());
    Assert.assertEquals(3, IndexFormatVersion.CURRENT.minVectorFeatureVersion());
  }
}
