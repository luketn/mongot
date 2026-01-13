package com.xgen.testing.util;

import com.google.errorprone.annotations.Var;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class ManuallyUpdatedClock extends Clock {
  @Var private Clock clock;
  @Var private Duration offset;

  public ManuallyUpdatedClock(Clock clock) {
    this.clock = clock;
    this.offset = Duration.ZERO;
  }

  private Clock get() {
    return Clock.offset(this.clock, this.offset);
  }

  public void update(Duration updateDuration) {
    this.offset = this.offset.plus(updateDuration);
  }

  public void toFixed(Instant instant) {
    this.clock = Clock.fixed(instant, ZoneId.systemDefault());
    this.offset = Duration.ZERO;
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
