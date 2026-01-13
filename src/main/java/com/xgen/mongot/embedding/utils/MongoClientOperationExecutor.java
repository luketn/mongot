package com.xgen.mongot.embedding.utils;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.util.mongodb.Errors;
import com.xgen.mongot.util.retry.ExponentialBackoffPolicy;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedRunnable;
import net.jodah.failsafe.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class for executing MongoDB operations with a retry policy and basic latency and
 * success/failure metrics. Ideally to be only used with idempotent operations where we know the
 * operation is retryable. Also note that the retry policy only works for top level exceptions /
 * errors - bulk operations like bulkWrite which can produce partial failures need to be handled
 * appropriately by the caller.
 */
public class MongoClientOperationExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(MongoClientOperationExecutor.class);
  private final RetryPolicy<Object> retryPolicy;
  private final MetricsFactory metricsFactory;
  private final String requestLatencyMetricName;
  private final String failedRequestsMetricName;
  private final String successfulRequestsMetricName;

  /**
   * Creates a new MongoClientOperationExecutor.
   *
   * @param metricsFactory the metrics factory to use for creating metrics.
   * @param resourceName the mongoDB resource name. This will be used as the prefix for all metrics.
   */
  public MongoClientOperationExecutor(MetricsFactory metricsFactory, String resourceName) {
    this.metricsFactory = metricsFactory;

    this.retryPolicy =
        ExponentialBackoffPolicy.builder()
            .initialDelay(Duration.ofMillis(500))
            .backoffFactor(2)
            .maxDelay(Duration.ofMillis(5000))
            .maxRetries(5)
            .jitter(0.1)
            .build()
            .applyParameters(
                new RetryPolicy<>()
                    .handleIf(
                        ex -> ex instanceof MongoException && isRetryable((MongoException) ex))
                    .onRetry(
                        ex ->
                            LOG.warn(
                                "Operation failed. Attempt count {}",
                                ex.getAttemptCount(),
                                ex.getLastFailure())));
    this.requestLatencyMetricName = resourceName + ".requestLatency";
    this.failedRequestsMetricName = resourceName + ".failedRequests";
    this.successfulRequestsMetricName = resourceName + ".successfulRequests";
  }

  /**
   * Execute a MongoDB operation with retries and operation-specific metrics. Note that the retries
   * will be performed on the caller's thread so this is best suited for short-lived operations.
   *
   * @param operationName Name of the operation (e.g., "bulkWrite", "readCheckpoint")
   * @param operation The MongoDB operation to execute
   */
  public <T> T execute(String operationName, CheckedSupplier<T> operation) throws Exception {

    var metricTags = Tags.of("operation", operationName);
    Timer timer = this.metricsFactory.timer(this.requestLatencyMetricName, metricTags);
    Counter successCounter =
        this.metricsFactory.counter(this.successfulRequestsMetricName, metricTags);
    Counter failureCounter = this.metricsFactory.counter(this.failedRequestsMetricName, metricTags);

    try {
      // The latency metric here includes retries.
      T result = Timed.supplier(timer, () -> Failsafe.with(this.retryPolicy).get(operation));

      successCounter.increment();
      return result;

    } catch (Exception e) {
      failureCounter.increment();
      throw e;
    }
  }

  public void execute(String operationName, CheckedRunnable operation) throws Exception {
    execute(
        operationName,
        () -> {
          operation.run();
          return null;
        });
  }

  private static boolean isRetryable(MongoException e) {
    if (e instanceof MongoSocketException
        || e instanceof MongoNotPrimaryException
        || e instanceof MongoNodeIsRecoveringException
        || e instanceof MongoCursorNotFoundException
        || e instanceof MongoTimeoutException) {
      return true;
    }

    return Errors.RETRYABLE_ERROR_CODES.contains(e.getCode());
  }
}
