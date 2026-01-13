package com.xgen.mongot.server.http;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.server.grpc.HealthManager;
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
  private HttpClient client;

  public HttpResponse sendRequest(boolean isGetRequest) throws IOException, InterruptedException {
    String localhostIP = "http://0.0.0.0:";
    URI uri =
        URI.create(
            localhostIP + this.server.getAddress().getPort() + HealthCheckServer.ENDPOINT_PATH);
    @Var HttpRequest req = HttpRequest.newBuilder().uri(uri).GET().build();
    if (!isGetRequest) {
      req = HttpRequest.newBuilder().uri(uri).DELETE().build();
    }
    return this.client.send(req, HttpResponse.BodyHandlers.ofString());
  }

  @Test
  public void testHealthCheckServingResponse() throws IOException, InterruptedException {
    when(this.healthManager.isHealthy()).thenReturn(true);

    var response = sendRequest(true);
    Assert.assertEquals("{\"status\":\"SERVING\"}", response.body());
  }

  @Test
  public void testHealthCheckNotServingResponse() throws IOException, InterruptedException {
    when(this.healthManager.isHealthy()).thenReturn(false);
    var response = sendRequest(true);
    Assert.assertEquals("{\"status\":\"NOT_SERVING\"}", response.body());
  }

  @Test
  public void testHealthCheckBadRequest() throws IOException, InterruptedException {
    var response = sendRequest(false);
    Assert.assertEquals("{\"error\":\"BAD_REQUEST\"}", response.body());
  }

  @Test
  public void testAddressInUse() {
    Assert.assertThrows(
        RuntimeException.class,
        () -> HealthCheckServer.create(this.server.getAddress(), this.healthManager));
  }

  @Before
  public void start() {
    this.healthManager = mock(HealthManager.class);
    this.server = HealthCheckServer.create(new InetSocketAddress("0.0.0.0", 0), this.healthManager);
    this.client = HttpClient.newBuilder().build();
    this.server.start();
  }

  @After
  public void stop() {
    this.server.stop();
  }
}
