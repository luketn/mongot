package com.xgen.mongot.cursor;

import io.micrometer.core.instrument.Timer;

@FunctionalInterface
public interface QueryBatchTimerRecorder {
  // called to record the given Timer under this index's batch processing metrics
  void recordSample(Timer.Sample sample);
}
