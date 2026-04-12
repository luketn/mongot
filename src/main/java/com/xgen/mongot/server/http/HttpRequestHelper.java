package com.xgen.mongot.server.http;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.sun.net.httpserver.HttpExchange;
import com.xgen.mongot.util.CollectionUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

/** Helper methods for request handling from the HTTP Health Check Server */
public class HttpRequestHelper {

  public static final Responses RESPONSES =
      new Responses(
          new Response(
              200, JsonNodeFactory.instance.objectNode().put("status", "SERVING").toString()),
          new Response(
              503, JsonNodeFactory.instance.objectNode().put("status", "NOT_SERVING").toString()),
          new Response(
              400, JsonNodeFactory.instance.objectNode().put("error", "BAD_REQUEST").toString()),
          new Response(
              500,
              JsonNodeFactory.instance.objectNode().put("status", "INTERNAL_ERROR").toString()));

  /**
   * Parses the URI query parameters from the HTTP request.
   *
   * @return a map of query parameter names and values.
   * @throws Exception if there were duplicate URI arguments in the request
   */
  public Map<String, String> parseQueryParams(HttpExchange httpExchange) throws Exception {
    Optional<String> query = Optional.ofNullable(httpExchange.getRequestURI()).map(URI::getQuery);
    try {
      return query
          .map(q -> URLEncodedUtils.parse(q, UTF_8))
          .orElse(Collections.emptyList())
          .stream()
          .collect(
              CollectionUtils.toMapUniqueKeys(NameValuePair::getName, NameValuePair::getValue));
    } catch (IllegalStateException e) { // thrown when duplicate keys added to the result map
      throw new DuplicateArgumentException(e);
    }
  }

  /** Responds to the caller with the {@link Response} object as a serialized HTTP response. */
  public void sendJsonResponse(HttpExchange httpExchange, Response response) throws IOException {
    httpExchange.getResponseHeaders().set("Content-Type", "application/json");
    httpExchange.sendResponseHeaders(response.code, response.body.getBytes(UTF_8).length);
    try (OutputStream os = httpExchange.getResponseBody()) {
      os.write(response.body.getBytes(UTF_8));
    }
  }

  /** Returns true if this is a GET request, else false. */
  public boolean isGetRequest(HttpExchange httpExchange) {
    return "GET".equals(httpExchange.getRequestMethod());
  }

  public record Responses(
      Response serving, Response notServing, Response badRequest, Response internalError) {}

  public record Response(int code, String body) {}

  static class DuplicateArgumentException extends Exception {
    public DuplicateArgumentException(Exception e) {
      super(e);
    }
  }
}
