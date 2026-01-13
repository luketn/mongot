package com.xgen.mongot.util;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class DurationUtilsTest {

  @Test
  public void testGetRandomlyDistributionDuration() {

    var durations =
        List.of(
            Duration.ofSeconds(20),
            Duration.ofMinutes(5),
            Duration.ofMinutes(30),
            Duration.ofHours(3),
            Duration.ofDays(10));

    var fractions = List.of(0.1, 0.2, 0.4, 0.9);

    BiConsumer<Duration, Double> check =
        (ttl, fraction) -> {
          var randTimeToLive = DurationUtils.getRandomlyDistributionDuration(ttl, fraction);

          assertTrue(randTimeToLive.toSeconds() > 0);
          assertTrue(verifyValueInRange(randTimeToLive, ttl, fraction));
        };

    // run 100 times per pair
    durations.forEach(
        ttl -> {
          fractions.forEach(
              fraction -> {
                IntStream.range(0, 100).forEach(i -> check.accept(ttl, fraction));
              });
        });
  }

  @Test
  public void testFractionEqualsZero() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> DurationUtils.getRandomlyDistributionDuration(Duration.ofMinutes(1), 0.0));
  }

  @Test
  public void testInvalidFractionValuesShouldThrow() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> DurationUtils.getRandomlyDistributionDuration(Duration.ofMinutes(1), -1));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> DurationUtils.getRandomlyDistributionDuration(Duration.ofMinutes(1), 1.0));

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> DurationUtils.getRandomlyDistributionDuration(Duration.ofMinutes(1), 1.3));
  }

  @Test
  public void testRandomFullDistributionDurationNegativeValueThrows() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> DurationUtils.getRandomFullDistributionDuration(Duration.ofMinutes(-1)));
  }

  @Test
  public void testRandomFullDistributionDurationZeroValueThrows() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> DurationUtils.getRandomFullDistributionDuration(Duration.ofMinutes(0)));
  }

  @Test
  public void testRandomFullDistributionDurationInRange() {
    var durations =
        List.of(
            Duration.ofSeconds(20),
            Duration.ofMinutes(5),
            Duration.ofMinutes(30),
            Duration.ofMillis(51),
            Duration.ofMillis(202));

    Consumer<Duration> check =
        (givenValue) -> {
          long randomValue = DurationUtils.getRandomFullDistributionDuration(givenValue).toNanos();
          assertTrue(randomValue >= 0);
          assertTrue(randomValue <= 2 * givenValue.toNanos());
        };

    // run 100 times per pair
    durations.forEach(d -> IntStream.range(0, 100).forEach(i -> check.accept(d)));
  }

  private boolean verifyValueInRange(Duration randTimeToLive, Duration ttl, double fraction) {
    long delta = (long) (ttl.toSeconds() * fraction);
    long upperBound = ttl.toSeconds() + delta;
    long lowerBound = ttl.toSeconds() - delta;

    return randTimeToLive.toSeconds() >= lowerBound && randTimeToLive.toSeconds() < upperBound;
  }
}
