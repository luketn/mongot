package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class ConditionTest {

  @Test(timeout = 1000)
  public void shouldWaitForCondition() {

    var counter = new AtomicLong();

    CompletableFuture.runAsync(
        () -> {
          try {
            Thread.sleep(500);
            counter.incrementAndGet();
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
        });

    Condition.await()
        .atMost(Duration.ofHours(Integer.MAX_VALUE))
        .withPollingInterval(Duration.ofMillis(50))
        .until(() -> counter.get() == 1);
  }

  @Test(expected = IllegalStateException.class, timeout = 1000)
  public void shouldFailOnTimeout() {
    Condition.await()
        .atMost(Duration.ofMillis(200))
        .withPollingInterval(Duration.ofMillis(50))
        .until(() -> false);
  }

  @Test(expected = IllegalStateException.class, timeout = 1000)
  public void testTerminationOnConditionEvaluationFailure() {
    Condition.await()
        .atMost(Duration.ofHours(Integer.MAX_VALUE))
        .withPollingInterval(Duration.ofMillis(50))
        .until(
            () -> {
              throw new IllegalStateException();
            });
  }

  @Test(timeout = 60_000)
  public void testUntilNoThrowSucceeds() throws InterruptedException {
    var counter = new AtomicLong();
    Condition.await()
        .atMost(Duration.ofMinutes(1))
        .withPollingInterval(Duration.ofMillis(50))
        .untilDoesNotThrow(
            () -> {
              if (counter.incrementAndGet() < 5) {
                fail("~~ oops ~~");
              }
            });
  }

  @Test(timeout = 60_000)
  public void testUntilNoThrowFails() {
    var builder =
        Condition.await().atMost(Duration.ofSeconds(2)).withPollingInterval(Duration.ofMillis(50));

    var exception =
        assertThrows(
            AssertionError.class, () -> builder.untilDoesNotThrow(() -> fail("~~ oops ~~")));
    assertEquals("~~ oops ~~", exception.getCause().getMessage());
  }
}
