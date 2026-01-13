package com.xgen.mongot.index.lucene.explain.tracing;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.information.ResourceUsageCollector;
import io.opentelemetry.context.Context;
import java.util.HashSet;
import java.util.Set;
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
}
