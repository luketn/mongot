package com.xgen.mongot.util.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.ServerStatusDataExtractor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for creating {@link ConnectionPoolListener}s that track and emit connection pool
 * metrics into a provided {@link MeterRegistry}.
 */
@SuppressWarnings("ClassCanBeRecord")
public class InstrumentedConnectionPoolListenerFactory {

  private static final Tags METRIC_TAGS =
      Tags.of(ServerStatusDataExtractor.Scope.REPLICATION.getTag());
  @VisibleForTesting static final String METRIC_NAMESPACE = "mongoClient.connectionPool";

  private final MetricsFactory metricsFactory;

  public InstrumentedConnectionPoolListenerFactory(MeterRegistry meterRegistry) {
    this.metricsFactory = new MetricsFactory(METRIC_NAMESPACE, meterRegistry, METRIC_TAGS);
  }

  public ConnectionPoolListener createFor(String clientName) {
    var clientMetrics =
        new Metrics(this.metricsFactory, METRIC_TAGS.and(Tag.of("clientName", clientName)));

    return new InstrumentedConnectionPoolListener(clientMetrics);
  }

  private record Metrics(
      AtomicLong minSize,
      AtomicLong maxSize,
      AtomicLong connections,
      AtomicLong connectionsCheckedOut) {

    private Metrics(MetricsFactory metricsFactory, Tags tags) {
      this(
          metricsFactory.numGauge("minSize", tags),
          metricsFactory.numGauge("maxSize", tags),
          metricsFactory.numGauge("connections", tags),
          metricsFactory.numGauge("connectionsCheckedOut", tags));
    }
  }

  private static final class InstrumentedConnectionPoolListener implements ConnectionPoolListener {

    private static final Logger LOG =
        LoggerFactory.getLogger(InstrumentedConnectionPoolListener.class);

    private final Metrics metrics;
    private final ConcurrentMap<ServerId, ConnectionPoolSize> poolSizeByServerId;

    private InstrumentedConnectionPoolListener(Metrics metrics) {
      this.metrics = metrics;

      this.poolSizeByServerId = new ConcurrentHashMap<>();
    }

    /** Invoked when a connection pool is created. */
    @Override
    public void connectionPoolCreated(ConnectionPoolCreatedEvent event) {
      var minSize = event.getSettings().getMinSize();
      var maxSize = event.getSettings().getMaxSize();

      // The connection pool size is not provided by the pool closed event, so we persist the pool
      // size here for use in the pool closed event.
      ConnectionPoolSize poolSize = new ConnectionPoolSize(minSize, maxSize);
      Optional<ConnectionPoolSize> previous =
          Optional.ofNullable(this.poolSizeByServerId.putIfAbsent(event.getServerId(), poolSize));
      if (previous.isPresent()) {
        ConnectionPoolSize previousPoolSize = previous.get();
        LOG.atWarn()
            .addKeyValue("serverId", event.getServerId())
            .addKeyValue("minSize", poolSize.minSize())
            .addKeyValue("maxSize", poolSize.maxSize())
            .addKeyValue("previousMinSize", previousPoolSize.minSize())
            .addKeyValue("previousMaxSize", previousPoolSize.maxSize())
            .log(
                "Duplicate connection pool created event for the same server id, "
                    + "metrics may be inaccurate.");
        return;
      }

      this.metrics.minSize.addAndGet(minSize);
      this.metrics.maxSize.addAndGet(maxSize);
    }

    /** Invoked when a connection pool is closed. */
    @Override
    public void connectionPoolClosed(@NotNull ConnectionPoolClosedEvent event) {
      Optional.ofNullable(this.poolSizeByServerId.remove(event.getServerId()))
          .ifPresentOrElse(
              poolSize -> {
                this.metrics.minSize.addAndGet(-poolSize.minSize());
                this.metrics.maxSize.addAndGet(-poolSize.maxSize());
              },
              () ->
                  LOG.atWarn()
                      .addKeyValue("serverId", event.getServerId())
                      .log(
                          "Connection pool closed event received for unknown or already closed "
                              + "server id. Metrics may be inaccurate."));
    }

    /** Invoked when a connection is created. */
    @Override
    public void connectionCreated(@NotNull ConnectionCreatedEvent event) {
      this.metrics.connections.incrementAndGet();
    }

    /** Invoked when a connection is removed from a pool. */
    @Override
    public void connectionClosed(@NotNull ConnectionClosedEvent event) {
      this.metrics.connections.decrementAndGet();
    }

    /** Invoked when a connection is checked out of a pool. */
    @Override
    public void connectionCheckedOut(@NotNull ConnectionCheckedOutEvent event) {
      this.metrics.connectionsCheckedOut.incrementAndGet();
    }

    /** Invoked when a connection is checked in to a pool. */
    @Override
    public void connectionCheckedIn(@NotNull ConnectionCheckedInEvent event) {
      this.metrics.connectionsCheckedOut.decrementAndGet();
    }

    private record ConnectionPoolSize(int minSize, int maxSize) {}
  }
}
