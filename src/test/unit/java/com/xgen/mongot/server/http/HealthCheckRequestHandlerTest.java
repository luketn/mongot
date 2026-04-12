package com.xgen.mongot.server.http;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpExchange;
import com.xgen.mongot.server.grpc.HealthManager;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class HealthCheckRequestHandlerTest {

  private HttpRequestHelper requestHelper;
  private HealthManager healthManager;
  private HealthCheckRequestHandler handler;
  private HttpExchange exchange;

  @Before
  public void setUp() {
    this.requestHelper = mock(HttpRequestHelper.class);
    this.healthManager = mock(HealthManager.class);
    this.handler = new HealthCheckRequestHandler(this.requestHelper, this.healthManager);
    this.exchange = mock(HttpExchange.class);
  }

  @Test
  public void testHandleHealthyRequest() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange)).thenReturn(Collections.emptyMap());
    when(this.healthManager.isHealthy()).thenReturn(true);

    this.handler.handle(this.exchange);

    verify(this.healthManager).isHealthy();
    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.serving());
  }

  @Test
  public void testHandleUnhealthyRequest() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange)).thenReturn(Collections.emptyMap());
    when(this.healthManager.isHealthy()).thenReturn(false);

    this.handler.handle(this.exchange);

    verify(this.healthManager).isHealthy();
    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.notServing());
  }

  @Test
  public void testHandleNonGetRequest() throws IOException {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(false);

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.badRequest());
  }

  @Test
  public void testHandleWithQueryParams() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange))
        .thenReturn(Map.of("someParam", "value"));

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.badRequest());
  }

  @Test
  public void testHandleExceptionDuringHealthCheck() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange)).thenReturn(Collections.emptyMap());
    when(this.healthManager.isHealthy()).thenThrow(new RuntimeException("Health check failed"));

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.internalError());
  }

  @Test
  public void testHandleExceptionDuringValidation() throws IOException {
    when(this.requestHelper.isGetRequest(this.exchange))
        .thenThrow(new RuntimeException("Validation failed"));

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.internalError());
  }

  @Test
  public void testHandleDuplicateQueryParams() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange))
        .thenThrow(new HttpRequestHelper.DuplicateArgumentException(new IllegalStateException()));

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.badRequest());
  }
}
