package com.xgen.mongot.server.http;

import com.google.common.annotations.VisibleForTesting;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.xgen.mongot.server.grpc.HealthManager;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An HTTP server used by mongot community to accept healthcheck and readiness check probes. */
public class HealthCheckServer {
  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckServer.class);

  private final NamedExecutorService healthCheckExecutor;
  private final HttpServer server;
  private final Map<String, HttpHandler> requestHandlers;

  private HealthCheckServer(
      HttpServer server, MeterRegistry meterRegistry, Map<String, HttpHandler> requestHandlers) {
    this.server = server;
    this.requestHandlers = requestHandlers;
    this.healthCheckExecutor =
        Executors.fixedSizeThreadScheduledExecutor("HealthCheckServer", 2, meterRegistry);
  }

  public static HealthCheckServer create(
      InetSocketAddress address,
      MeterRegistry meterRegistry,
      HealthManager healthManager,
      ReadinessChecker readinessChecker) {
    return Crash.because("failed to start health check server")
        .ifThrows(
            () -> {
              try {
                HttpRequestHelper helper = new HttpRequestHelper();
                Map<String, HttpHandler> handlers =
                    Map.of(
                        "/health", new HealthCheckRequestHandler(helper, healthManager),
                        "/ready", new ReadinessCheckRequestHandler(helper, readinessChecker));

                HttpServer server = HttpServer.create(address, 0);
                return new HealthCheckServer(server, meterRegistry, handlers);
              } catch (BindException e) {
                LOG.atError()
                    .addKeyValue("address", address)
                    .setCause(e)
                    .log(
                        "Error creating health check server, Address already in use,"
                            + " please configure a different address for health check server");
                throw e;
              }
            });
  }

  /** Starts up the health check server. */
  public void start() {
    LOG.atInfo().addKeyValue("address", getAddress()).log("Starting health check server...");
    this.requestHandlers.forEach(this.server::createContext);
    this.server.setExecutor(this.healthCheckExecutor);
    this.server.start();
  }

  /** Shuts down the health check server and it's backing threadpool. */
  public void stop() {
    LOG.info("Shutting down health check server...");
    Crash.because("failed to shut down health check server").ifThrows(() -> this.server.stop(0));
    Executors.shutdownOrFail(this.healthCheckExecutor);
  }

  @VisibleForTesting
  public InetSocketAddress getAddress() {
    return this.server.getAddress();
  }
}
