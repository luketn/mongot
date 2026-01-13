package com.xgen.mongot.cursor.batch;

import org.junit.Assert;
import org.junit.Test;

public class ConstantBatchSizeStrategyTest {

  @Test
  public void testConstantBatchSize() {
    var strategy = new ConstantBatchSizeStrategy();
    var firstBatchSize = strategy.adviseNextBatchSize();
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(firstBatchSize, strategy.adviseNextBatchSize());
    }
  }
}
