package com.xgen.mongot.monitor;

import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ReplicationStateMonitor {
  private final Gate initialSyncGate;
  private final Gate replicationGate;

  private ReplicationStateMonitor(Gate initialSyncGate, Gate replicationGate) {
    this.initialSyncGate = initialSyncGate;
    this.replicationGate = replicationGate;
  }

  public Gate getInitialSyncGate() {
    return this.initialSyncGate;
  }

  public Gate getReplicationGate() {
    return this.replicationGate;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ReplicationStateMonitor enabled() {
    return new ReplicationStateMonitor(ToggleGate.opened(), ToggleGate.opened());
  }

  public static ReplicationStateMonitor disabled() {
    return new ReplicationStateMonitor(ToggleGate.closed(), ToggleGate.closed());
  }

  public static ReplicationStateMonitor diskBased(
      HysteresisConfig replicationConfig,
      Optional<HysteresisConfig> initialSyncConfig,
      DiskMonitor diskMonitor) {
    var replicationGate =
        new HysteresisGate(replicationConfig.openThreshold(), replicationConfig.closeThreshold());
    diskMonitor.register(replicationGate);

    Gate initialSyncGate;
    if (initialSyncConfig.isPresent()) {
      var config = initialSyncConfig.get();
      initialSyncGate = new HysteresisGate(config.openThreshold(), config.closeThreshold());
      diskMonitor.register(initialSyncGate);
    } else {
      initialSyncGate = ToggleGate.opened();
    }

    return builder()
        .setReplicationGate(replicationGate)
        .setInitialSyncGate(initialSyncGate)
        .build();
  }

  public static class Builder {
    Optional<Gate> initialSyncGate = Optional.empty();
    Optional<Gate> replicationGate = Optional.empty();

    public Builder setInitialSyncGate(Gate gate) {
      this.initialSyncGate = Optional.of(gate);
      return this;
    }

    public Builder setReplicationGate(Gate gate) {
      this.replicationGate = Optional.of(gate);
      return this;
    }

    public ReplicationStateMonitor build() {
      var initialSyncGate = Check.isPresent(this.initialSyncGate, "initialSyncGate");
      var replicationGate = Check.isPresent(this.replicationGate, "replicationGate");
      return new ReplicationStateMonitor(initialSyncGate, replicationGate);
    }
  }
}
