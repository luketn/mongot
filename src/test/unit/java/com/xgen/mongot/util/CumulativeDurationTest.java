package com.xgen.mongot.util;

import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;

public class CumulativeDurationTest {

  @Test
  public void testRunnableExecutionDurationAccumulation() throws InterruptedException {
    var duration = new CumulativeDuration();
    duration.accumulate(() -> Thread.sleep(100));
    duration.accumulate(() -> Thread.sleep(200));
    Assert.assertTrue(Duration.ofMillis(300).minus(duration.getTotal()).isNegative());
  }

  @Test
  public void testSupplierExecutionDurationAccumulation() throws InterruptedException {
    var duration = new CumulativeDuration();
    duration.accumulate(
        () -> {
          Thread.sleep(100);
          return 0;
        });
    duration.accumulate(
        () -> {
          Thread.sleep(200);
          return 0;
        });
    Assert.assertTrue(Duration.ofMillis(300).minus(duration.getTotal()).isNegative());
  }
}
