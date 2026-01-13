package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.definition.NormalizerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.testing.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class NormalizerDefinitionTest {

  @Test
  public void testGetName() {
    NormalizerDefinition definition =
        NormalizerDefinition.stockNormalizer(StockNormalizerName.NONE);

    Assert.assertEquals(StockNormalizerName.NONE.getNormalizerName(), definition.name());
  }

  @Test
  public void testEquals() {
    TestUtils.assertEqualityGroups(
        () -> NormalizerDefinition.stockNormalizer(StockNormalizerName.NONE),
        () -> NormalizerDefinition.stockNormalizer(StockNormalizerName.LOWERCASE));
  }
}
