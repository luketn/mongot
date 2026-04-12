package com.xgen.mongot.server.http;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpExchange;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class ReadinessCheckRequestHandlerTest {

  private HttpRequestHelper requestHelper;
  private ReadinessChecker readinessChecker;
  private ReadinessCheckRequestHandler handler;
  private HttpExchange exchange;

  @Before
  public void setUp() {
    this.requestHelper = mock(HttpRequestHelper.class);
    this.readinessChecker = mock(ReadinessChecker.class);
    this.handler = new ReadinessCheckRequestHandler(this.requestHelper, this.readinessChecker);
    this.exchange = mock(HttpExchange.class);
  }

  @Test
  public void testHandleReadyRequest() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange)).thenReturn(Collections.emptyMap());
    when(this.readinessChecker.isReady(false)).thenReturn(true);

    this.handler.handle(this.exchange);

    verify(this.readinessChecker).isReady(false);
    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.serving());
  }

  @Test
  public void testHandleNotReadyRequest() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange)).thenReturn(Collections.emptyMap());
    when(this.readinessChecker.isReady(false)).thenReturn(false);

    this.handler.handle(this.exchange);

    verify(this.readinessChecker).isReady(false);
    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.notServing());
  }

  @Test
  public void testHandleWithAllowFailedIndexesTrue() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange))
        .thenReturn(Map.of("allowFailedIndexes", "true"));
    when(this.readinessChecker.isReady(true)).thenReturn(true);
    when(this.readinessChecker.isReady(false)).thenReturn(false);

    this.handler.handle(this.exchange);

    verify(this.readinessChecker).isReady(true);
    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.serving());
  }

  @Test
  public void testHandleWithAllowFailedIndexesFalse() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange))
        .thenReturn(Map.of("allowFailedIndexes", "false"));
    when(this.readinessChecker.isReady(false)).thenReturn(true);
    when(this.readinessChecker.isReady(true)).thenReturn(false);

    this.handler.handle(this.exchange);

    verify(this.readinessChecker).isReady(false);
    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.serving());
  }

  @Test
  public void testHandleWithAllowFailedIndexesCaseInsensitive() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange))
        .thenReturn(Map.of("allowFailedIndexes", "TRUE"));
    when(this.readinessChecker.isReady(true)).thenReturn(true);
    when(this.readinessChecker.isReady(false)).thenReturn(false);

    this.handler.handle(this.exchange);

    verify(this.readinessChecker).isReady(true);
  }

  @Test
  public void testHandleWithInvalidAllowFailedIndexesValue() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange))
        .thenReturn(Map.of("allowFailedIndexes", "invalid"));

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.badRequest());
  }

  @Test
  public void testHandleWithInvalidQueryParam() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange))
        .thenReturn(Map.of("invalidParam", "value"));

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.badRequest());
  }

  @Test
  public void testHandleNonGetRequest() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(false);

    this.handler.handle(this.exchange);

    verify(this.requestHelper)
        .sendJsonResponse(this.exchange, HttpRequestHelper.RESPONSES.badRequest());
  }

  @Test
  public void testHandleExceptionDuringProcessing() throws Exception {
    when(this.requestHelper.isGetRequest(this.exchange)).thenReturn(true);
    when(this.requestHelper.parseQueryParams(this.exchange)).thenReturn(Collections.emptyMap());
    when(this.readinessChecker.isReady(false)).thenThrow(new RuntimeException("Test exception"));

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
