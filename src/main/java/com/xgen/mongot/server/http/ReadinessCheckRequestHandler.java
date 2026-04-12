package com.xgen.mongot.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler for handling k8s readiness probes to determine if the server is ready to start
 * receiving traffic. This check waits for all indexes to be built and queryable to avoid the server
 * being prematurely placed behind the LB causing requests routed to this server to fail.
 */
public class ReadinessCheckRequestHandler implements HttpHandler {

  public static final Logger LOG = LoggerFactory.getLogger(ReadinessCheckRequestHandler.class);

  private static final String ALLOW_FAILED_INDEXES_ARG = "allowFailedIndexes";
  private static final Set<String> VALID_READINESS_KEYS = Set.of(ALLOW_FAILED_INDEXES_ARG);

  private final HttpRequestHelper requestHelper;
  private final ReadinessChecker readinessChecker;

  public ReadinessCheckRequestHandler(
      HttpRequestHelper requestHelper, ReadinessChecker readinessChecker) {
    this.requestHelper = requestHelper;
    this.readinessChecker = readinessChecker;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    try {
      ValidationResponse validationResponse = validateRequest(exchange);
      if (!validationResponse.success) {
        this.requestHelper.sendJsonResponse(exchange, HttpRequestHelper.RESPONSES.badRequest());
        return;
      }

      this.requestHelper.sendJsonResponse(
          exchange,
          this.readinessChecker.isReady(validationResponse.allowFailedIndexes)
              ? HttpRequestHelper.RESPONSES.serving()
              : HttpRequestHelper.RESPONSES.notServing());
    } catch (Exception e) {
      LOG.warn("Error processing readiness check http request", e);
      this.requestHelper.sendJsonResponse(exchange, HttpRequestHelper.RESPONSES.internalError());
    }
  }

  private ValidationResponse validateRequest(HttpExchange exchange) {
    if (!this.requestHelper.isGetRequest(exchange)) {
      return new ValidationResponse(false, false);
    }
    Map<String, String> queryParams;
    try {
      queryParams = this.requestHelper.parseQueryParams(exchange);
    } catch (Exception e) {
      LOG.warn("Error parsing query params", e);
      return new ValidationResponse(false, false);
    }

    if (!VALID_READINESS_KEYS.containsAll(queryParams.keySet())) {
      return new ValidationResponse(false, false);
    }

    if (queryParams.containsKey(ALLOW_FAILED_INDEXES_ARG)) {
      String allowedFailedIndexesInput = queryParams.get(ALLOW_FAILED_INDEXES_ARG);
      if ("true".equalsIgnoreCase(allowedFailedIndexesInput)) {
        return new ValidationResponse(true, true);
      } else if ("false".equalsIgnoreCase(allowedFailedIndexesInput)) {
        return new ValidationResponse(true, false);
      } else {
        return new ValidationResponse(false, false);
      }
    }
    return new ValidationResponse(true, false);
  }

  private record ValidationResponse(boolean success, boolean allowFailedIndexes) {}
}
