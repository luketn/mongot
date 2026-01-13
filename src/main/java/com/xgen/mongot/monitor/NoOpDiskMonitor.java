package com.xgen.mongot.monitor;

import java.time.Duration;

public class NoOpDiskMonitor implements DiskMonitor {
  @Override
  public void register(Gate gate) {}

  @Override
  public void start(Duration diskMonitorDuration) {}

  @Override
  public void stop() {}
}
