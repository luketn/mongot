package com.xgen.mongot.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.xgen.mongot.server.grpc.HealthManager;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler used by the k8s healthcheck probes to determine that the server is still healthy
 * and available.
 */
public class HealthCheckRequestHandler implements HttpHandler {
  public static final Logger LOG = LoggerFactory.getLogger(HealthCheckRequestHandler.class);

  private final HttpRequestHelper requestHelper;
  private final HealthManager healthManager;

  public HealthCheckRequestHandler(HttpRequestHelper requestHelper, HealthManager healthManager) {
    this.requestHelper = requestHelper;
    this.healthManager = healthManager;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    try {
      if (!validateRequest(exchange)) {
        this.requestHelper.sendJsonResponse(exchange, HttpRequestHelper.RESPONSES.badRequest());
        return;
      }

      this.requestHelper.sendJsonResponse(
          exchange,
          this.healthManager.isHealthy()
              ? HttpRequestHelper.RESPONSES.serving()
              : HttpRequestHelper.RESPONSES.notServing());
    } catch (Exception e) {
      LOG.warn("Error processing health check http request", e);
      this.requestHelper.sendJsonResponse(exchange, HttpRequestHelper.RESPONSES.internalError());
    }
  }

  private boolean validateRequest(HttpExchange exchange) {
    if (!this.requestHelper.isGetRequest(exchange)) {
      return false;
    }

    Map<String, String> queryParams;
    try {
      queryParams = this.requestHelper.parseQueryParams(exchange);
    } catch (Exception e) {
      LOG.warn("Error parsing query params", e);
      return false;
    }

    return queryParams.isEmpty();
  }
}
