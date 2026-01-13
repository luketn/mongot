package com.xgen.mongot.cursor.batch;

import com.google.errorprone.annotations.Var;
import com.xgen.testing.mongot.cursor.batch.BatchCursorOptionsBuilder;
import org.junit.Assert;
import org.junit.Test;

public class ExponentiallyIncreasingBatchSizeStrategyTest {

  @Test
  public void testExponentialIncrease() {
    var strategy = new ExponentiallyIncreasingBatchSizeStrategy();
    var cursorOptions = BatchCursorOptionsBuilder.builder().build();
    @Var var previousBatchSize = 0;
    for (int i = 0; i < 10; i++) {
      var nextBatchSize = strategy.adviseNextBatchSize();
      Assert.assertTrue(previousBatchSize < nextBatchSize);
      previousBatchSize = nextBatchSize;
      strategy.adjust(cursorOptions);
    }
  }
}
