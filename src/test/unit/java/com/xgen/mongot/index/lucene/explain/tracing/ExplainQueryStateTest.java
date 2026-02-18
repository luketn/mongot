package com.xgen.mongot.index.lucene.explain.tracing;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.information.ResourceUsageCollector;
import io.opentelemetry.context.Context;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import oshi.SystemInfo;
import oshi.software.os.OSThread;
import oshi.software.os.OperatingSystem;

public class ExplainQueryStateTest {

  @Test
  public void testVerbosity() {
    try (var unusedScope =
        Context.current()
            .with(new ExplainQueryState(new Explain.QueryInfo(Explain.Verbosity.QUERY_PLANNER), 0))
            .makeCurrent()) {
      Assert.assertFalse(
          ExplainQueryState.getFromContext()
              .get()
              .getQueryInfo()
              .getVerbosity()
              .isGreaterThan(Explain.Verbosity.EXECUTION_STATS));
      Assert.assertFalse(
          ExplainQueryState.getFromContext()
              .get()
              .getQueryInfo()
              .getVerbosity()
              .isGreaterThan(Explain.Verbosity.ALL_PLANS_EXECUTION));
    }
  }

  @Test
  public void testGetFromContextUnavailable() {
    Assert.assertTrue(ExplainQueryState.getFromContext().isEmpty());
  }

  @Test
  public void testGetFromContextAvailable() {
    try (var unusedScope =
        Context.current()
            .with(new ExplainQueryState(new Explain.QueryInfo(Explain.Verbosity.QUERY_PLANNER), 0))
            .makeCurrent()) {
      Assert.assertTrue(ExplainQueryState.getFromContext().isPresent());
    }
  }

  @Test
  public void testDoNotCreateIndexPartitionForDefault() {
    try (var unusedScope =
        Context.current()
            .with(
                new ExplainQueryState(
                    new Explain.QueryInfo(Explain.Verbosity.QUERY_PLANNER),
                    IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))
            .makeCurrent()) {
      Assert.assertTrue(ExplainQueryState.getFromContext().isPresent());
      Assert.assertThrows(
          AssertionError.class,
          () ->
              ExplainQueryState.getFromContext()
                  .get()
                  .enterIndexPartitionQueryContext(
                      IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()));
    }
  }

  @Test
  public void testCreateQuerySubContexts() {
    int numPartitions = 15;
    try (var unusedScope =
        Context.current()
            .with(
                new ExplainQueryState(
                    new Explain.QueryInfo(Explain.Verbosity.QUERY_PLANNER), numPartitions))
            .makeCurrent()) {
      Assert.assertTrue(ExplainQueryState.getFromContext().isPresent());
      Set<Explain.QueryInfo> subQueryInfos = new HashSet<>();

      for (int i = 0; i < numPartitions; i++) {
        try (var subContextScope =
            ExplainQueryState.getFromContext().get().enterIndexPartitionQueryContext(i)) {
          Assert.assertTrue(ExplainQueryState.getFromContext().isPresent());
          Assert.assertNotEquals(
              ExplainQueryState.getFromContext().get().getQueryInfo(),
              ExplainQueryState.getFromContext().get().getRootQueryInfo());
          subQueryInfos.add(ExplainQueryState.getFromContext().get().getQueryInfo());
        }
      }

      Assert.assertEquals(numPartitions, subQueryInfos.size());
    }
  }

  @Test
  public void testWrapExecutor() {
    SystemInfo systemInfo = Mockito.mock(SystemInfo.class);
    OperatingSystem operatingSystem = Mockito.mock(OperatingSystem.class);
    OSThread thread = Mockito.mock(OSThread.class);
    when(systemInfo.getOperatingSystem()).thenReturn(operatingSystem);
    when(operatingSystem.getCurrentThread()).thenReturn(thread);
    when(thread.updateAttributes()).thenReturn(true);
    // Stats are obtained when each Runnable completes (or getResourceUsage() call).
    when(thread.getMajorFaults()).thenReturn(1L, 2L, 1L, 2L);

    var state = new ExplainQueryState(systemInfo);
    state
        .wrap(
            command -> {
              command.run();
            })
        .execute(() -> {});
    ResourceUsageCollector usage = state.getResourceUsageExplainer().getResourceUsages().getFirst();
    assertEquals(2, usage.majorFaults());
  }

  /**
   * Tests that concurrent access to partition contexts correctly isolates each partition's
   * QueryInfo.
   */
  @Test
  public void testConcurrentPartitionContextsAreIsolated() throws Exception {
    int numPartitions = 8;
    int iterations = 100; // Run multiple times to increase chance of hitting race

    for (int iter = 0; iter < iterations; iter++) {
      ExplainQueryState explainState =
          new ExplainQueryState(
              new Explain.QueryInfo(Explain.Verbosity.EXECUTION_STATS), numPartitions);

      try (var rootScope = Context.current().with(explainState).makeCurrent()) {
        ExecutorService executor = Executors.newFixedThreadPool(numPartitions);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numPartitions);
        ConcurrentHashMap<Integer, Explain.QueryInfo> capturedQueryInfos =
            new ConcurrentHashMap<>();
        AtomicInteger queryInfoMismatches = new AtomicInteger(0);
        AtomicReference<Exception> unexpectedException = new AtomicReference<>(null);

        // Launch all threads - they will wait at the startLatch
        for (int partition = 0; partition < numPartitions; partition++) {
          int partitionId = partition;
          executor.submit(
              () -> {
                try {
                  // Wait for all threads to be ready
                  startLatch.await();

                  // Enter this partition's context
                  try (var partitionScope =
                      explainState.enterIndexPartitionQueryContext(partitionId)) {
                    // Capture the QueryInfo we get - should be partition-specific
                    Explain.QueryInfo queryInfo = explainState.getQueryInfo();
                    capturedQueryInfos.put(partitionId, queryInfo);

                    // Record a unique marker to this partition's FeatureExplainer
                    // to verify data goes to the right place
                    queryInfo.getFeatureExplainer(
                        TestFeatureExplainer.class, () -> new TestFeatureExplainer(partitionId));

                    // Verify we can still get the same QueryInfo after recording
                    Explain.QueryInfo queryInfoAgain = explainState.getQueryInfo();
                    if (queryInfo != queryInfoAgain) {
                      queryInfoMismatches.incrementAndGet();
                    }

                    // Small delay to increase interleaving
                    Thread.sleep(1);

                    // Verify once more before exiting
                    Explain.QueryInfo queryInfoFinal = explainState.getQueryInfo();
                    if (queryInfo != queryInfoFinal) {
                      queryInfoMismatches.incrementAndGet();
                    }
                  }
                } catch (Exception e) {
                  // Capture first unexpected exception - this is not expected in normal operation
                  unexpectedException.compareAndSet(null, e);
                } finally {
                  doneLatch.countDown();
                }
              });
        }

        // Release all threads at once to maximize concurrency
        startLatch.countDown();

        // Wait for completion
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Fail immediately if any unexpected exception occurred
        if (unexpectedException.get() != null) {
          Assert.fail(
              "Unexpected exception during concurrent execution: " + unexpectedException.get());
        }

        // Verify no race condition mismatches occurred
        Assert.assertEquals(
            "QueryInfo mismatches during concurrent execution (race condition detected)",
            0,
            queryInfoMismatches.get());

        // Verify each partition got a unique QueryInfo
        Assert.assertEquals(
            "Each partition should have captured a QueryInfo",
            numPartitions,
            capturedQueryInfos.size());

        // Verify all QueryInfos are distinct (not the same object)
        Set<Explain.QueryInfo> uniqueInfos = new HashSet<>(capturedQueryInfos.values());
        Assert.assertEquals(
            "Each partition should have a distinct QueryInfo object",
            numPartitions,
            uniqueInfos.size());

        // Verify none of them are the root QueryInfo
        for (Explain.QueryInfo info : capturedQueryInfos.values()) {
          Assert.assertNotEquals(
              "Partition QueryInfo should not be root QueryInfo",
              explainState.getRootQueryInfo(),
              info);
        }

        // Verify each partition's FeatureExplainer has the correct partition ID
        for (int partition = 0; partition < numPartitions; partition++) {
          Explain.QueryInfo info = capturedQueryInfos.get(partition);
          Optional<TestFeatureExplainer> explainer =
              info.getFeatureExplainer(TestFeatureExplainer.class);
          Assert.assertTrue(
              "Partition " + partition + " should have TestFeatureExplainer",
              explainer.isPresent());
          Assert.assertEquals(
              "Partition " + partition + " should have recorded its own partition ID",
              partition,
              explainer.get().getPartitionId());
        }
      }
    }
  }

  /** A simple test FeatureExplainer that records which partition created it. */
  private static class TestFeatureExplainer implements FeatureExplainer {
    private final int partitionId;

    TestFeatureExplainer(int partitionId) {
      this.partitionId = partitionId;
    }

    int getPartitionId() {
      return this.partitionId;
    }

    @Override
    public void emitExplanation(
        Explain.Verbosity verbosity,
        com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder builder) {
      // No-op for test purposes
    }
  }
}
