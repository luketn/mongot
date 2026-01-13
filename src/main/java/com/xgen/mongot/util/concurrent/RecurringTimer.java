package com.xgen.mongot.util.concurrent;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/** A thread-safe timer which becomes ready exactly once every period. */
public class RecurringTimer {

  private final Clock clock;
  private final Duration period;
  private final AtomicReference<Instant> lastUp;

  @VisibleForTesting
  RecurringTimer(Clock clock, Duration period) {
    this.clock = clock;
    this.period = period;
    this.lastUp = new AtomicReference<>(Instant.now(clock));
  }

  public static RecurringTimer every(Duration period) {
    return new RecurringTimer(Clock.systemUTC(), period);
  }

  /**
   * Whether or not the timer has elapsed. Returns {@code true} if and only if no other call to
   * {@code isUp()} returned {@code true} within the last {@code period}.
   */
  public boolean isUp() {
    Instant lastUpInstant = this.lastUp.get();
    Instant nextUpInstant = lastUpInstant.plus(this.period);
    Instant now = Instant.now(this.clock);

    return now.isAfter(nextUpInstant) && this.lastUp.compareAndSet(lastUpInstant, now);
  }
}
