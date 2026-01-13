package com.xgen.mongot.util;

import org.slf4j.Logger;

/**
 * A wrapper of a regular {@link Runnable} to proxy the invocations and log uncaught exceptions and
 * errors using the supplied {@link Logger}.
 */
public interface VerboseRunnable extends Runnable {

  @Override
  default void run() {
    try {
      verboseRun();
    } catch (Throwable t) {
      getLogger().error("Runnable failed", t);
      throw t;
    }
  }

  void verboseRun();

  Logger getLogger();
}
