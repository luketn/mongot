package com.xgen.mongot.util.concurrent;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class RecurringTimerTest {

  private static class SwappableFixedClock extends Clock {

    private final Clock beforeClock;
    private final Clock afterClock;

    private volatile boolean swapped;

    private SwappableFixedClock(Instant beforeInstant, Instant afterInstant) {
      this.beforeClock = Clock.fixed(beforeInstant, ZoneId.systemDefault());
      this.afterClock = Clock.fixed(afterInstant, ZoneId.systemDefault());

      this.swapped = false;
    }

    private Clock get() {
      return this.swapped ? this.afterClock : this.beforeClock;
    }

    public void tick() {
      this.swapped = true;
    }

    @Override
    public ZoneId getZone() {
      return get().getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return get().withZone(zone);
    }

    @Override
    public Instant instant() {
      return get().instant();
    }
  }

  @Test
  public void testIsUpSimple() {
    SwappableFixedClock clock = new SwappableFixedClock(Instant.MIN, Instant.MIN.plusSeconds(1));
    RecurringTimer timer = new RecurringTimer(clock, Duration.ofMillis(500));

    Assert.assertFalse(timer.isUp());
    clock.tick();
    Assert.assertTrue(timer.isUp());
    Assert.assertFalse(timer.isUp());
  }

  @Test
  public void testIsUpConcurrent() throws InterruptedException {
    SwappableFixedClock clock = new SwappableFixedClock(Instant.MIN, Instant.MIN.plusSeconds(1));
    RecurringTimer timer = new RecurringTimer(clock, Duration.ofMillis(500));

    AtomicInteger tickCount = new AtomicInteger(0);

    // Spawn tasks that try to increment tickCount.
    int numTasks = 12;
    ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(numTasks);
    Stream<Runnable> incrementers =
        IntStream.range(0, numTasks)
            .mapToObj(
                i ->
                    () -> {
                      try {
                        //noinspection InfiniteLoopStatement
                        while (true) {
                          //noinspection BusyWait
                          Thread.sleep(1);
                          if (timer.isUp()) {
                            tickCount.getAndIncrement();
                          }
                        }
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    });
    incrementers.forEach(threadPoolExecutor::submit);

    // Wait a bit, flip the clock, wait a bit more.
    Thread.sleep(50);
    clock.tick();
    Thread.sleep(50);

    // Shutdown the thread pool.
    threadPoolExecutor.shutdownNow();
    threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS);

    Assert.assertEquals(1, tickCount.get());
  }
}
