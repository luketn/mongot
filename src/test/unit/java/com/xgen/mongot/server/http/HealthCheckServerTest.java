package com.xgen.mongot.server.http;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.server.grpc.HealthManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HealthCheckServerTest {
  private HealthCheckServer server;
  private HealthManager healthManager;
  private ReadinessChecker readinessChecker;
  private MeterRegistry meterRegistry;
  private HttpClient client;

  public HttpResponse sendRequest(String path) throws IOException, InterruptedException {
    String localhostIP = "http://0.0.0.0:";
    URI uri = URI.create(localhostIP + this.server.getAddress().getPort() + path);
    HttpRequest req = HttpRequest.newBuilder().uri(uri).GET().build();
    return this.client.send(req, HttpResponse.BodyHandlers.ofString());
  }

  @Test
  public void testHealthCheckPath() throws Exception {
    when(this.healthManager.isHealthy()).thenReturn(true);
    when(this.readinessChecker.isReady(anyBoolean())).thenReturn(false);

    var response = sendRequest("/health");
    Assert.assertEquals(200, response.statusCode());
    Assert.assertEquals("{\"status\":\"SERVING\"}", response.body());
  }

  @Test
  public void testHealthCheckPathWhenHealthManagerThrowsException()
      throws IOException, InterruptedException {
    when(this.healthManager.isHealthy()).thenThrow(new RuntimeException("Health check failed"));

    var response = sendRequest("/health");
    Assert.assertEquals(500, response.statusCode());
    Assert.assertEquals("{\"status\":\"INTERNAL_ERROR\"}", response.body());
  }

  @Test
  public void testReadinessCheckPath() throws Exception {
    when(this.healthManager.isHealthy()).thenReturn(false);
    when(this.readinessChecker.isReady(anyBoolean())).thenReturn(true);

    var response = sendRequest("/ready");
    Assert.assertEquals(200, response.statusCode());
    Assert.assertEquals("{\"status\":\"SERVING\"}", response.body());
  }

  @Test
  public void testReadinessCheckPathWhenReadinessCheckerThrowsException() throws Exception {
    doThrow(new RuntimeException("Readiness check failed"))
        .when(this.readinessChecker)
        .isReady(anyBoolean());

    var response = sendRequest("/ready");
    Assert.assertEquals(500, response.statusCode());
    Assert.assertEquals("{\"status\":\"INTERNAL_ERROR\"}", response.body());
  }

  @Test
  public void testBadPath() throws IOException, InterruptedException {
    var response = sendRequest("/foobar");
    Assert.assertEquals(404, response.statusCode());
  }

  @Test
  public void testAddressInUse() {
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            HealthCheckServer.create(
                this.server.getAddress(),
                this.meterRegistry,
                this.healthManager,
                this.readinessChecker));
  }

  @Before
  public void start() {
    this.healthManager = mock(HealthManager.class);
    this.readinessChecker = mock(ReadinessChecker.class);
    this.meterRegistry = new SimpleMeterRegistry();
    this.server =
        HealthCheckServer.create(
            new InetSocketAddress("0.0.0.0", 0),
            this.meterRegistry,
            this.healthManager,
            this.readinessChecker);
    this.client = HttpClient.newBuilder().build();
    this.server.start();
  }

  @After
  public void stop() {
    this.server.stop();
  }
}
