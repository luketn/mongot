package com.xgen.mongot.util.timers;

import com.google.common.base.Stopwatch;
import com.google.common.truth.Truth;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class CpuTickerTest {
  private boolean isLinux() {
    return System.getProperty("os.name").toLowerCase().contains("linux");
  }

  @Test
  public void testCpuTicker() {
    Truth.assertThat(CpuTicker.INSTANCE.isPresent()).isEqualTo(isLinux());
    if (CpuTicker.INSTANCE.isPresent()) {
      var cpuTime = Stopwatch.createStarted(CpuTicker.INSTANCE.get());
      for (long i = 0; i < Long.MAX_VALUE; i++) {
        if (cpuTime.elapsed(TimeUnit.MILLISECONDS) > 10) {
          break;
        }
      }
      Truth.assertThat(cpuTime.elapsed(TimeUnit.MILLISECONDS)).isAtLeast(10L);
    }
  }
}
