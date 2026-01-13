package com.xgen.mongot.util;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Shutdown {

  private static final Logger LOG = LoggerFactory.getLogger(Shutdown.class);

  private static final Duration SHUTDOWN_ABORT_TIMEOUT = Duration.ofMinutes(5);

  /** Registers a list of actions to be run when the JVM is shutting down. */
  public static void registerHook(Runnable... actions) {
    Runnable shutdownHook =
        () -> {
          // Set the shutdown flag on the Crash object
          Crash.setShutdownStartedFlag();
          Thread.currentThread().setName("shutdown-hook");
          startShutdownAbortThread();

          LOG.info("Received shutdown signal. Shutting down.");

          for (Runnable action : actions) {
            action.run();
          }
        };

    try {
      Runtime.INSTANCE.addShutdownHook(shutdownHook);
    } catch (IllegalStateException e) {
      Crash.setShutdownStartedFlag();
      Crash.because("Cannot add shutdown hooks when system is already shutting down")
          .withThrowable(e)
          .now();
    }
  }

  @Crash.SafeWithoutCrashLog
  private static void startShutdownAbortThread() {
    Thread shutdownAbortThread =
        new Thread(
            () -> {
              Crash.because("interrupted waiting for shutdown to complete")
                  .withThreadDump()
                  .ifThrows(() -> Thread.sleep(SHUTDOWN_ABORT_TIMEOUT.toMillis()));
              Crash.because(
                      "timed out waiting for graceful shutdown to complete, aborting shutdown")
                  .withThreadDump()
                  .withoutCrashLog("do not create a crash log for this harmless crash")
                  .now();
            },
            "shutdown-abort-thread");

    shutdownAbortThread.setDaemon(true);
    shutdownAbortThread.start();
  }
}
