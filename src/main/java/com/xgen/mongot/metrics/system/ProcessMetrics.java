package com.xgen.mongot.metrics.system;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;

// TODO(CLOUDP-285787): investigate moving metrics (especially gauges) to MetricsFactory
public class ProcessMetrics {
  private final OSProcess self;

  ProcessMetrics(OSProcess self) {
    this.self = self;
  }

  static ProcessMetrics create(SystemInfo systemInfo, MeterRegistry meterRegistry) {
    OSProcess self = systemInfo.getOperatingSystem().getCurrentProcess();
    Gauge.builder("system.process.majorPageFaults", self, OSProcess::getMajorFaults)
        .description("Number of major page faults")
        .baseUnit(BaseUnits.OPERATIONS)
        .register(meterRegistry);
    Gauge.builder("system.process.minorPageFaults", self, OSProcess::getMinorFaults)
        .description("Number of minor page faults")
        .baseUnit(BaseUnits.OPERATIONS)
        .register(meterRegistry);
    return new ProcessMetrics(self);
  }

  public void update() {
    this.self.updateAttributes();
  }
}
