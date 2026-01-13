package com.xgen.mongot.server.executors;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import org.bson.BsonDocument;

/**
 * Executes provided commands on corresponding thread pools, implementing the Bulkhead pattern. This
 * behavior allows grouping of homogeneous tasks and isolates blocking calls from response-time
 * sensitive tasks to guarantee predictable execution time under load.
 */
public class BulkheadCommandExecutor implements Closeable {

  private final NamedExecutorService blockingCommandExecutor;

  public BulkheadCommandExecutor(MeterRegistry meterRegistry) {
    this.blockingCommandExecutor =
        Executors.unboundedCachingThreadPool("blocking-server-worker", meterRegistry);
  }

  /**
   * Schedules given command according to its execution policy.
   *
   * <p>If an exception is thrown in {@link Command#run}, this method won't throw the exception. The
   * returned object will wrap the exception as {@code cause}. Calling {@link CompletableFuture#get}
   * on the returned object will throw a {@link java.util.concurrent.ExecutionException} that wraps
   * the exception in {@link Command#run}.
   */
  public CompletableFuture<BsonDocument> execute(Command command) {
    return switch (command.getExecutionPolicy()) {
      case ASYNC -> CompletableFuture.supplyAsync(command::run, this.blockingCommandExecutor);
      case SYNC -> {
        try {
          yield CompletableFuture.completedFuture(command.run());
        } catch (Throwable t) {
          yield CompletableFuture.failedFuture(t);
        }
      }
    };
  }

  @Override
  public void close() {
    Executors.shutdownOrFail(this.blockingCommandExecutor);
  }
}
