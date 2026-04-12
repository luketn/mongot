package com.xgen.mongot.server.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class HttpRequestHelperTest {

  private HttpRequestHelper helper;
  private HttpExchange exchange;

  @Before
  public void setUp() {
    this.helper = new HttpRequestHelper();
    this.exchange = mock(HttpExchange.class);
  }

  @Test
  public void testParseQueryParamsEmpty() throws Exception {
    when(this.exchange.getRequestURI()).thenReturn(new URI("http://localhost/path"));

    Map<String, String> params = this.helper.parseQueryParams(this.exchange);

    assertTrue(params.isEmpty());
  }

  @Test
  public void testParseQueryParamsSingle() throws Exception {
    when(this.exchange.getRequestURI()).thenReturn(new URI("http://localhost/path?key=value"));

    Map<String, String> params = this.helper.parseQueryParams(this.exchange);

    assertEquals(1, params.size());
    assertEquals("value", params.get("key"));
  }

  @Test
  public void testParseQueryParamsMultiple() throws Exception {
    when(this.exchange.getRequestURI())
        .thenReturn(new URI("http://localhost/path?key1=value1&key2=value2&key3=value3"));

    Map<String, String> params = this.helper.parseQueryParams(this.exchange);

    assertEquals(3, params.size());
    assertEquals("value1", params.get("key1"));
    assertEquals("value2", params.get("key2"));
    assertEquals("value3", params.get("key3"));
  }

  @Test
  public void testParseQueryParamsWithEncodedValues() throws Exception {
    when(this.exchange.getRequestURI())
        .thenReturn(new URI("http://localhost/path?key=hello%20world&special=%21%40%23"));

    Map<String, String> params = this.helper.parseQueryParams(this.exchange);

    assertEquals(2, params.size());
    assertEquals("hello world", params.get("key"));
    assertEquals("!@#", params.get("special"));
  }

  @Test
  public void testParseQueryParamsNullUri() throws Exception {
    when(this.exchange.getRequestURI()).thenReturn(null);

    Map<String, String> params = this.helper.parseQueryParams(this.exchange);

    assertTrue(params.isEmpty());
  }

  @Test
  public void testParseQueryParamsWithDuplicateKeys() throws URISyntaxException {
    when(this.exchange.getRequestURI())
        .thenReturn(new URI("http://localhost/path?key=value1&key=value2"));

    assertThrows(
        HttpRequestHelper.DuplicateArgumentException.class,
        () -> this.helper.parseQueryParams(this.exchange));
  }

  @Test
  public void testParseQueryParamsWithDuplicateKeysAmongMultiple() throws URISyntaxException {
    when(this.exchange.getRequestURI())
        .thenReturn(new URI("http://localhost/path?key1=value1&key2=value2&key1=value3"));

    assertThrows(
        HttpRequestHelper.DuplicateArgumentException.class,
        () -> this.helper.parseQueryParams(this.exchange));
  }

  @Test
  public void testSendJsonResponse() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Headers headers = new Headers();

    when(this.exchange.getResponseHeaders()).thenReturn(headers);
    when(this.exchange.getResponseBody()).thenReturn(outputStream);

    HttpRequestHelper.Response response =
        new HttpRequestHelper.Response(200, "{\"status\":\"OK\"}");
    this.helper.sendJsonResponse(this.exchange, response);

    assertEquals("application/json", headers.getFirst("Content-Type"));
    verify(this.exchange).sendResponseHeaders(200, "{\"status\":\"OK\"}".getBytes().length);
    assertEquals("{\"status\":\"OK\"}", outputStream.toString());
  }

  @Test
  public void testSendJsonResponseWithDifferentStatusCodes() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Headers headers = new Headers();

    when(this.exchange.getResponseHeaders()).thenReturn(headers);
    when(this.exchange.getResponseBody()).thenReturn(outputStream);

    HttpRequestHelper.Response response =
        new HttpRequestHelper.Response(404, "{\"error\":\"Not Found\"}");
    this.helper.sendJsonResponse(this.exchange, response);

    verify(this.exchange).sendResponseHeaders(404, "{\"error\":\"Not Found\"}".getBytes().length);
    assertEquals("{\"error\":\"Not Found\"}", outputStream.toString());
  }

  @Test
  public void testIsGetRequestTrue() {
    when(this.exchange.getRequestMethod()).thenReturn("GET");

    assertTrue(this.helper.isGetRequest(this.exchange));
  }

  @Test
  public void testIsGetRequestFalse() {
    when(this.exchange.getRequestMethod()).thenReturn("POST");

    assertFalse(this.helper.isGetRequest(this.exchange));
  }

  @Test
  public void testIsGetRequestOtherMethods() {
    when(this.exchange.getRequestMethod()).thenReturn("PUT");
    assertFalse(this.helper.isGetRequest(this.exchange));

    when(this.exchange.getRequestMethod()).thenReturn("DELETE");
    assertFalse(this.helper.isGetRequest(this.exchange));

    when(this.exchange.getRequestMethod()).thenReturn("PATCH");
    assertFalse(this.helper.isGetRequest(this.exchange));
  }

  @Test
  public void testResponsesConstants() {
    assertEquals(400, HttpRequestHelper.RESPONSES.badRequest().code());
    assertTrue(HttpRequestHelper.RESPONSES.badRequest().body().contains("BAD_REQUEST"));

    assertEquals(200, HttpRequestHelper.RESPONSES.serving().code());
    assertTrue(HttpRequestHelper.RESPONSES.serving().body().contains("SERVING"));

    assertEquals(503, HttpRequestHelper.RESPONSES.notServing().code());
    assertTrue(HttpRequestHelper.RESPONSES.notServing().body().contains("NOT_SERVING"));

    assertEquals(500, HttpRequestHelper.RESPONSES.internalError().code());
    assertTrue(HttpRequestHelper.RESPONSES.internalError().body().contains("INTERNAL_ERROR"));
  }
}
