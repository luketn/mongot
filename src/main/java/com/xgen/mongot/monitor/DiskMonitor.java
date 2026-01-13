package com.xgen.mongot.monitor;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;

/**
 * DiskMonitor monitors disk usage and updates all registered gates with the observed values. Any
 * gates opened through this process indicate allowing disk write activity.
 */
public interface DiskMonitor {
  /**
   * Registers a gate to be updated every time disk utilization is computed. Any gate opening means
   * write activity is being allowed.
   *
   * @param gate gate to be registered.
   */
  void register(Gate gate);

  /**
   * Starts disk monitoring at the given interval.
   *
   * @param diskMonitorDuration interval for monitoring iterations
   */
  @VisibleForTesting
  void start(Duration diskMonitorDuration);

  /** Stops disk monitoring processes. */
  void stop();
}
