package com.xgen.mongot.index.lucene.explain.tracing;

import com.xgen.mongot.index.lucene.explain.information.ResourceUsageCollector;
import com.xgen.mongot.index.lucene.explain.information.ResourceUsageOutput;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.annotation.concurrent.GuardedBy;
import oshi.software.os.OSThread;

/**
 * This FeatureExplainer collects the resource usage of the command thread as well as the optional
 * resource usage of all the Lucene threads used to execute the query. This class is thread-safe.
 */
class ResourceUsageFeatureExplainer implements FeatureExplainer {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUsageFeatureExplainer.class);

  private static class ResourceUsageCollectorGuard implements AutoCloseable {
    private final OSThread thread;
    private final BiConsumer<ResourceUsageCollector, Integer> reporter;
    private final int batchNumber;
    private final AtomicBoolean reportedPreviously;

    public ResourceUsageCollectorGuard(
        SystemInfo systemInfo,
        BiConsumer<ResourceUsageCollector, Integer> reporter,
        int batchNumber) {
      this.thread = systemInfo.getOperatingSystem().getCurrentThread();
      this.reporter = reporter;
      this.batchNumber = batchNumber;
      this.reportedPreviously = new AtomicBoolean(false);
    }

    // Gets resource usage since the guard was created or the last call to
    // ResourceUsageCollectorGuard.
    synchronized ResourceUsageCollector getResourceUsageDelta() {
      var majorFaults = this.thread.getMajorFaults();
      var minorFaults = this.thread.getMinorFaults();
      var userTimeMs = this.thread.getUserTime();
      var systemTimeMs = this.thread.getKernelTime();
      var reportingThreads = this.reportedPreviously.getAndSet(true) ? 0 : 1;
      if (!this.thread.updateAttributes()) {
        LOG.atWarn()
            .addKeyValue("threadId", this.thread.getThreadId())
            .log("Could not refresh usage for thread");
        return ResourceUsageCollector.EMPTY;
      }
      return new ResourceUsageCollector(
          this.thread.getMajorFaults() - majorFaults,
          this.thread.getMinorFaults() - minorFaults,
          this.thread.getUserTime() - userTimeMs,
          this.thread.getKernelTime() - systemTimeMs,
          reportingThreads);
    }

    @Override
    public void close() {
      this.reporter.accept(getResourceUsageDelta(), this.batchNumber);
    }
  }

  private final SystemInfo systemInfo;
  private final ThreadLocal<ResourceUsageCollectorGuard> currentThreadUsage;
  private final ThreadLocal<Integer> currentBatch;
  private final AtomicInteger numBatches;

  @GuardedBy("this")
  private final List<ResourceUsageCollector> totalUsages;

  ResourceUsageFeatureExplainer(SystemInfo systemInfo) {
    this.systemInfo = systemInfo;
    this.numBatches = new AtomicInteger(0);
    this.totalUsages = new ArrayList<>(List.of(ResourceUsageCollector.EMPTY));
    this.currentBatch = ThreadLocal.withInitial(this.numBatches::get);
    this.currentThreadUsage =
        ThreadLocal.withInitial(
            () ->
                new ResourceUsageCollectorGuard(
                    this.systemInfo, this::addThreadUsage, this.currentBatch.get()));
  }

  /**
   * Updates the state of numBatches/totalUsages for the new batch. Resets the current thread in
   * case the same thread is used across multiple search/getMore requests and sets the currentBatch.
   * This method can be invoked concurrently.
   */
  synchronized void refreshCurrentThreadAndUpdateState() {
    this.numBatches.getAndIncrement();
    this.totalUsages.add(ResourceUsageCollector.EMPTY);
    this.currentBatch.set(this.numBatches.get());
    this.currentThreadUsage.set(
        new ResourceUsageCollectorGuard(
            this.systemInfo, this::addThreadUsage, this.currentBatch.get()));
  }

  /**
   * Adds the ResourceUsageCollector of a thread used within Lucene when executing the query (not
   * the original request thread) to its batch-specific ResourceUsageCollector.
   */
  private synchronized void addThreadUsage(ResourceUsageCollector usage, int batchNumber) {
    this.totalUsages.set(
        batchNumber, ResourceUsageCollector.sum(this.totalUsages.get(batchNumber), usage));
  }

  /**
   * Adds the current thread's ResourceUsageCollector to its batch's total ResourceUsageCollector
   * and returns all the ResourceUsageCollectors across all batches up till this point.
   */
  synchronized List<ResourceUsageCollector> getResourceUsages() {
    this.currentThreadUsage.get().close();
    return List.copyOf(this.totalUsages);
  }

  /** Wraps the runnable in order to update its batch-specific total ResourceUsageCollector. */
  Runnable wrap(Runnable runnable) {
    int localBatchNumber = this.currentBatch.get();

    return () -> {
      try (var unusedGuard =
          new ResourceUsageCollectorGuard(
              this.systemInfo, this::addThreadUsage, localBatchNumber)) {
        runnable.run();
      }
    };
  }

  /**
   * Wraps an input {@link Executor} so that all passed closures will report their resource usage
   * back to this object on completion.
   *
   * @return A resource usage recording <code>Executor</code>
   */
  Executor wrap(Executor executor) {
    return command -> executor.execute(wrap(command));
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    if (verbosity.isGreaterThan(Explain.Verbosity.QUERY_PLANNER)) {
      builder.resourceUsage(ResourceUsageOutput.create(getResourceUsages()));
    }
  }
}
