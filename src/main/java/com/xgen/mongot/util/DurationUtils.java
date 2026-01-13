package com.xgen.mongot.util;

import static com.xgen.mongot.util.Check.checkArg;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadLocalRandom;

public class DurationUtils {

  /**
   * Given a duration return a new random duration in the half open range of: {@code delta = value *
   * fraction; range = [value - delta, value + delta) }.
   *
   * @param value Given duration to evaluate
   * @param fraction randomization fraction based on given value.
   */
  public static Duration getRandomlyDistributionDuration(Duration value, double fraction) {
    Check.argIsPositive(fraction, "fraction");
    checkArg(fraction < 1.0, "fraction must be smaller than 1.0");

    long delta = (long) (value.toNanos() * fraction);
    long addition = ThreadLocalRandom.current().nextLong(2 * delta) - delta;
    return value.plus(addition, ChronoUnit.NANOS);
  }

  /**
   * Given a duration return a new random duration in the range of [0, 2 * duration]
   *
   * @param value Given duration to evaluate
   */
  public static Duration getRandomFullDistributionDuration(Duration value) {
    checkArg(value.isPositive(), "given base duration must be positive");

    long maximumBound = 2 * value.toNanos() + 1;
    return Duration.ofNanos(ThreadLocalRandom.current().nextLong(maximumBound));
  }
}
