package com.xgen.mongot.server.grpc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.config.manager.ConfigManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class HealthManagerTest {

  final MeterRegistry meterRegistry = new SimpleMeterRegistry();

  @Test
  public void testHealthManager() throws Exception {
    ConfigManager mockConfigManager = mock(ConfigManager.class);
    when(mockConfigManager.isReplicationInitialized()).thenReturn(false);
    HealthManager healthManager = new HealthManager(mockConfigManager, this.meterRegistry);

    // ConfigManager is not initialized, HealthManager should be unhealthy;
    Thread.sleep(2000);
    assertFalse(healthManager.isHealthy());

    when(mockConfigManager.isReplicationInitialized()).thenReturn(true);

    // ConfigManager is initialized, HealthManager should be healthy;
    Thread.sleep(2000);
    assertTrue(healthManager.isHealthy());

    // HealthManager is terminated, it's unhealthy now.
    healthManager.enterTerminalState();
    assertFalse(healthManager.isHealthy());
  }

  @Test
  public void testHealthManager_configManagerInitializedAfterServerTermination() throws Exception {
    ConfigManager mockConfigManager = mock(ConfigManager.class);
    when(mockConfigManager.isReplicationInitialized()).thenReturn(false);
    HealthManager healthManager = new HealthManager(mockConfigManager, this.meterRegistry);

    // HealthManager is terminated, it's unhealthy.
    healthManager.enterTerminalState();
    assertFalse(healthManager.isHealthy());

    when(mockConfigManager.isReplicationInitialized()).thenReturn(true);

    // Even when the ConfigManager is initialized, HealthManager should still be unhealthy.
    Thread.sleep(2000);
    assertFalse(healthManager.isHealthy());
  }
}
