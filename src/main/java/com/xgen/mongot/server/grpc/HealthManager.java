package com.xgen.mongot.server.grpc;

import static io.grpc.health.v1.HealthCheckResponse.ServingStatus.NOT_SERVING;
import static io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING;

import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.util.concurrent.Executors;
import io.grpc.BindableService;
import io.grpc.protobuf.services.HealthStatusManager;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manage health status of the gRPC server and export a service for health checks.
 *
 * <p>We will only consider the gRPC server healthy after the {@link ConfigManager} is initialized.
 */
public class HealthManager {
  private final ConfigManager configManager;
  private final HealthStatusManager healthStatusManager;
  private final ScheduledExecutorService statusUpdater;
  private volatile boolean healthy;
  private boolean terminated;

  public HealthManager(ConfigManager configManager, MeterRegistry meterRegistry) {
    this.configManager = configManager;
    this.healthStatusManager = new HealthStatusManager();
    this.healthStatusManager.setStatus(HealthStatusManager.SERVICE_NAME_ALL_SERVICES, NOT_SERVING);
    this.statusUpdater =
        Executors.singleThreadScheduledExecutor("grpc-health-check", meterRegistry);
    this.healthy = false;
    this.terminated = false;
    // Check whether configManager is initialized every second.
    this.statusUpdater.scheduleWithFixedDelay(this::updateStatus, 0, 1, TimeUnit.SECONDS);
  }

  public BindableService getHealthService() {
    return this.healthStatusManager.getHealthService();
  }

  public boolean isHealthy() {
    return this.healthy;
  }

  public synchronized void enterTerminalState() {
    this.terminated = true;
    this.healthStatusManager.enterTerminalState();
    this.healthy = false;
  }

  private synchronized void updateStatus() {
    // If the HealthManager is terminated, we don't need to update the status anymore.
    if (this.terminated) {
      this.statusUpdater.shutdown();
      return;
    }
    // If the replication is initialized, update the health status and shutdown the statusUpdater.
    if (this.configManager.isReplicationInitialized()) {
      this.healthy = true;
      this.healthStatusManager.setStatus(HealthStatusManager.SERVICE_NAME_ALL_SERVICES, SERVING);
      this.statusUpdater.shutdown();
    }
  }
}
