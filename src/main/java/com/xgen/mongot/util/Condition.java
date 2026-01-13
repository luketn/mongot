package com.xgen.mongot.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.Callable;

public class Condition {
  private static final int DEFAULT_POLLING_INTERVAL_MS = 50;

  public static class Builder {

    private Duration initialDelay = Duration.ZERO;
    private Duration postDelay = Duration.ZERO;
    private Duration pollingInterval = Duration.of(DEFAULT_POLLING_INTERVAL_MS, ChronoUnit.MILLIS);
    private Optional<Duration> atMost = Optional.empty();
    private Optional<Throwable> timeoutCause = Optional.empty();

    public Builder atMost(Duration atMost) {
      this.atMost = Optional.of(atMost);
      return this;
    }

    public Builder withInitialDelay(Duration delay) {
      this.initialDelay = delay;
      return this;
    }

    public Builder withPollingInterval(Duration interval) {
      this.pollingInterval = interval;
      return this;
    }

    public Builder withPostDelay(Duration delay) {
      this.postDelay = delay;
      return this;
    }

    public Builder withTimeoutCause(Throwable cause) {
      this.timeoutCause = Optional.of(cause);
      return this;
    }

    public void untilDoesNotThrow(CheckedRunnable<Exception> runnable) throws InterruptedException {
      Duration maxDuration = Check.isPresent(this.atMost, "atMost");

      Thread.sleep(this.initialDelay.toMillis());
      var start = Instant.now();

      while (true) {
        try {
          runnable.run();
          return;
        } catch (Throwable t) {
          Thread.sleep(this.postDelay.toMillis());
          if (Duration.between(start, Instant.now()).compareTo(maxDuration) >= 0) {
            throw new AssertionError(
                "Timed out waiting for success. The last exception is the cause of this one.", t);
          }
          Thread.sleep(this.pollingInterval.toMillis());
        }
      }
    }

    /**
     * Waits for condition to be `true` until the timeout. If any invocation results in a {@link
     * Exception}, the exception is wrapped and re-thrown.
     */
    public void until(Callable<Boolean> condition) {
      Duration maxDuration = Check.isPresent(this.atMost, "atMost");

      try {

        Thread.sleep(this.initialDelay.toMillis());
        var start = Instant.now();

        while (true) {
          if (condition.call()) {
            Thread.sleep(this.postDelay.toMillis());
            return;
          }
          if (Duration.between(start, Instant.now()).compareTo(maxDuration) >= 0) {
            if (this.timeoutCause.isPresent()) {
              throw new IllegalStateException(this.timeoutCause.get());
            }
            throw new IllegalStateException("Timed out while waiting for the condition");
          }
          Thread.sleep(this.pollingInterval.toMillis());
        }

      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public static Builder await() {
    return new Builder();
  }
}
