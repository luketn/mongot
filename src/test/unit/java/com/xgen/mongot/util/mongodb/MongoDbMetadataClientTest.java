package com.xgen.mongot.util.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.internal.MongoClientImpl;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class MongoDbMetadataClientTest {

  @Test
  public void testMongodUriUsedWhenMongosUriIsNotPresent() {
    var syncSource =
        new SyncSourceConfig(
            new ConnectionString("mongodb://user:pass@atlas-mongod:27017/"), // kingfisher:ignore
            Optional.empty(),
            new ConnectionString("mongodb://user:pass@atlas-mongod:27017/")); // kingfisher:ignore
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      String configuredHost =
          ((MongoClientImpl) client).getSettings().getClusterSettings().getHosts().get(0).getHost();
      Assert.assertTrue(configuredHost.contains("atlas-mongod"));
    }
  }

  @Test
  public void testMongosUriUsedWhenPresent() {
    var syncSource =
        new SyncSourceConfig(
            new ConnectionString("mongodb://user:pass@atlas-mongod:27017/"), // kingfisher:ignore
            Optional.of(
                new ConnectionString(
                    "mongodb://user:pass@atlas-mongos:27017/")), // kingfisher:ignore
            new ConnectionString("mongodb://user:pass@atlas-mongod:27017/")); // kingfisher:ignore
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      String configuredHost =
          ((MongoClientImpl) client).getSettings().getClusterSettings().getHosts().get(0).getHost();
      Assert.assertTrue(configuredHost.contains("atlas-mongos"));
    }
  }

  @Test
  public void testDefaultSocketTimeout() {
    var syncSource =
        new SyncSourceConfig(
            new ConnectionString("mongodb://user:pass@atlas-mongod:27017/"), // kingfisher:ignore
            Optional.empty(),
            new ConnectionString("mongodb://user:pass@atlas-mongod:27017/")); // kingfisher:ignore
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      Assert.assertEquals(
          10,
          ((MongoClientImpl) client)
              .getSettings()
              .getSocketSettings()
              .getReadTimeout(TimeUnit.SECONDS));
    }
  }

  @Test
  public void testSocketTimeoutIsNotOverriddenWhenSpecifiedInConnectionString() {
    var syncSource =
        new SyncSourceConfig(
            new ConnectionString(
                // kingfisher:ignore
                "mongodb://user:pass@atlas-mongod:27017/?socketTimeoutMS=30000"),
            Optional.empty(),
            new ConnectionString(
                // kingfisher:ignore
                "mongodb://user:pass@atlas-mongod:27017/?socketTimeoutMS=30000"));
    try (var client =
        MongoClientBuilder.buildNonReplicationPreferringMongos(
            syncSource, this.getClass().getSimpleName(), new SimpleMeterRegistry())) {
      Assert.assertEquals(
          30,
          ((MongoClientImpl) client)
              .getSettings()
              .getSocketSettings()
              .getReadTimeout(TimeUnit.SECONDS));
    }
  }

  @Test
  public void testUpdateMongoDbVersionMetric() throws Exception {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    MongoDbMetadataClient client = new MongoDbMetadataClient(Optional.empty(), meterRegistry);
    Method updateMethod =
        MongoDbMetadataClient.class.getDeclaredMethod("updateMongoDbVersionMetric");
    updateMethod.setAccessible(true);
    Field serverInfoField = MongoDbMetadataClient.class.getDeclaredField("mongoDbServerInfo");
    serverInfoField.setAccessible(true);

    // Test 1: Initial version (7.0.0)
    MongoDbVersion version700 = new MongoDbVersion(7, 0, 0);
    String version700String = version700.toString();
    serverInfoField.set(client, new MongoDbServerInfo(Optional.of(version700), Optional.empty()));
    updateMethod.invoke(client);

    Gauge gauge700 =
        meterRegistry.find("mongodb.version").tags("version", version700String).gauge();
    Assert.assertNotNull("Gauge for version 7.0.0 should exist", gauge700);
    Assert.assertEquals(1.0, gauge700.value(), 0.0);

    // Test 2: Version change (7.0.0 -> 8.1.0) - previous should be set to 0
    MongoDbVersion version810 = new MongoDbVersion(8, 1, 0);
    String version810String = version810.toString();
    serverInfoField.set(client, new MongoDbServerInfo(Optional.of(version810), Optional.empty()));
    updateMethod.invoke(client);

    Gauge gauge810 =
        meterRegistry.find("mongodb.version").tags("version", version810String).gauge();
    Assert.assertNotNull("Gauge for version 8.1.0 should exist", gauge810);
    Assert.assertEquals(1.0, gauge810.value(), 0.0);

    // Previous version (7.0.0) should now be 0
    Gauge previousGauge700 =
        meterRegistry.find("mongodb.version").tags("version", version700String).gauge();
    Assert.assertNotNull("Previous gauge for version 7.0.0 should still exist", previousGauge700);
    Assert.assertEquals(0.0, previousGauge700.value(), 0.0);

    // Test 3: Unknown version
    serverInfoField.set(client, new MongoDbServerInfo(Optional.empty(), Optional.empty()));
    updateMethod.invoke(client);

    Gauge gaugeUnknown = meterRegistry.find("mongodb.version").tags("version", "unknown").gauge();
    Assert.assertNotNull("Gauge for unknown version should exist", gaugeUnknown);
    Assert.assertEquals(1.0, gaugeUnknown.value(), 0.0);

    // Previous version (8.1.0) should now be 0
    Gauge previousGauge810 =
        meterRegistry.find("mongodb.version").tags("version", version810String).gauge();
    Assert.assertNotNull("Previous gauge for version 8.1.0 should still exist", previousGauge810);
    Assert.assertEquals(0.0, previousGauge810.value(), 0.0);

    client.close();
  }
}
