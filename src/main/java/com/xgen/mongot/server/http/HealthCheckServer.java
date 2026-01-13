package com.xgen.mongot.server.http;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.annotations.VisibleForTesting;
import com.sun.net.httpserver.HttpServer;
import com.xgen.mongot.server.grpc.HealthManager;
import com.xgen.mongot.util.Crash;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckServer {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckServer.class);
  protected static final String ENDPOINT_PATH = "/health";

  private final HttpServer server;
  private final HealthManager healthManager;
  private static final Responses responses =
      new Responses(
          JsonNodeFactory.instance.objectNode().put("error", "BAD_REQUEST").toString(),
          JsonNodeFactory.instance.objectNode().put("status", "SERVING").toString(),
          JsonNodeFactory.instance.objectNode().put("status", "NOT_SERVING").toString());

  private HealthCheckServer(HttpServer server, HealthManager healthManager) {
    this.server = server;
    this.healthManager = healthManager;
  }

  public static HealthCheckServer create(InetSocketAddress address, HealthManager healthManager) {
    return Crash.because("failed to start health check server")
        .ifThrows(
            () -> {
              try {
                HttpServer server = HttpServer.create(address, 0);
                return new HealthCheckServer(server, healthManager);
              } catch (BindException e) {
                LOG.atError()
                    .addKeyValue("address", address)
                    .setCause(e)
                    .log("Error creating health check server, Address already in use,"
                        + " please configure a different address for health check server");
                throw e;
              }
            });
  }

  @SuppressWarnings("checkstyle:MissingJavadocMethod")
  public void start() {
    LOG.atInfo()
        .addKeyValue("address", getAddress())
        .log("Starting health check server...");
    this.server.createContext(
        ENDPOINT_PATH,
        httpExchange -> {
          httpExchange.getResponseHeaders().set("Content-Type", "application/json");
          String response;
          if ("GET".equals(httpExchange.getRequestMethod())) {
            if (this.healthManager.isHealthy()) {
              response = responses.servingResponse();
              httpExchange.sendResponseHeaders(200, response.getBytes(UTF_8).length);
            } else {
              response = responses.notServingResponse();
              httpExchange.sendResponseHeaders(503, response.getBytes(UTF_8).length);
            }
          } else {
            response = responses.errorResponse();
            httpExchange.sendResponseHeaders(400, response.getBytes(UTF_8).length);
          }
          try (OutputStream os = httpExchange.getResponseBody()) {
            os.write(response.getBytes());
          }
        });
    this.server.setExecutor(null);
    this.server.start();
  }

  public void stop() {
    LOG.info("Shutting down health check server...");
    Crash.because("failed to shut down health check server")
        .ifThrows(
            () -> {
              this.server.stop(0);
            });
  }

  @VisibleForTesting
  public InetSocketAddress getAddress() {
    return this.server.getAddress();
  }

  private record Responses(
      String errorResponse, String servingResponse, String notServingResponse) {}
}
