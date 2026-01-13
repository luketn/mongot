package com.xgen.mongot.index.lucene.explain.tracing;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.ResourceUsageCollector;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import oshi.SystemInfo;
import oshi.software.os.OSThread;
import oshi.software.os.OperatingSystem;

public class ResourceUsageFeatureExplainerTest {
  private final SystemInfo systemInfo = Mockito.mock(SystemInfo.class);
  private final OperatingSystem operatingSystem = Mockito.mock(OperatingSystem.class);
  private final OSThread thread = Mockito.mock(OSThread.class);

  @Before
  public void setUp() {
    when(this.systemInfo.getOperatingSystem()).thenReturn(this.operatingSystem);
    when(this.operatingSystem.getCurrentThread()).thenReturn(this.thread);
    when(this.thread.updateAttributes()).thenReturn(true);
  }

  @Test
  public void testGetResourceUsage() {
    when(this.thread.getMajorFaults()).thenReturn(1L, 2L);
    when(this.thread.getMinorFaults()).thenReturn(3L, 4L);
    when(this.thread.getUserTime()).thenReturn(1000L, 1007L);
    when(this.thread.getKernelTime()).thenReturn(100L, 102L);

    var explainer = new ResourceUsageFeatureExplainer(this.systemInfo);
    ResourceUsageCollector usage = explainer.getResourceUsages().getFirst();
    assertEquals(1, usage.majorFaults());
    assertEquals(1, usage.minorFaults());
    assertEquals(7, usage.userTimeMs());
    assertEquals(2, usage.systemTimeMs());
    assertEquals(1, usage.reportingThreads(), 0.0);
  }

  @Test
  public void testUpdateThreadFails() {
    when(this.thread.getMajorFaults()).thenReturn(1L, 2L);
    when(this.thread.updateAttributes()).thenReturn(false);

    var explainer = new ResourceUsageFeatureExplainer(this.systemInfo);
    ResourceUsageCollector usage = explainer.getResourceUsages().getFirst();
    assertEquals(0, usage.majorFaults());
    assertEquals(0, usage.reportingThreads(), 0.0);
  }

  @Test
  public void testWrapExecutor() {
    // Stats are obtained when each Runnable completes (or getResourceUsage() call).
    when(this.thread.getMajorFaults()).thenReturn(1L, 2L, 1L, 2L);

    var explainer = new ResourceUsageFeatureExplainer(this.systemInfo);
    explainer
        .wrap(
            command -> {
              command.run();
            })
        .execute(() -> {});
    ResourceUsageCollector usage = explainer.getResourceUsages().getFirst();
    assertEquals(2, usage.majorFaults());
  }

  @Test
  public void testRefresh() {
    when(this.thread.getMajorFaults()).thenReturn(1L, 2L, 4L, 6L);

    var explainer = new ResourceUsageFeatureExplainer(this.systemInfo);
    var first = explainer.getResourceUsages();
    Truth.assertThat(first.getLast().majorFaults()).isEqualTo(1);

    explainer.refreshCurrentThreadAndUpdateState();
    var second = explainer.getResourceUsages();
    Truth.assertThat(second.size()).isEqualTo(2);
    Truth.assertThat(second.getLast().majorFaults()).isEqualTo(2);
  }

  @Test
  public void testConcurrentRefreshCurrentThreadAndUpdateState() throws InterruptedException {
    ResourceUsageFeatureExplainer explainer = new ResourceUsageFeatureExplainer(new SystemInfo());
    int numberOfThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < numberOfThreads; i++) {
      futures.add(
          executorService.submit(
              () -> {
                explainer.refreshCurrentThreadAndUpdateState();
                explainer.getResourceUsages();
              }));
    }

    executorService.shutdown();
    executorService.awaitTermination(5, TimeUnit.SECONDS);

    // One extra for the main test thread constructing the ResourceUsageFeatureExplainer
    assertEquals(numberOfThreads + 1, explainer.getResourceUsages().size());
  }

  @Test
  public void testEmitExplainMaxReportingThreadsIdempotent() {
    when(this.thread.getMajorFaults()).thenReturn(1L, 2L);
    when(this.thread.getMinorFaults()).thenReturn(3L, 4L);
    when(this.thread.getUserTime()).thenReturn(1000L, 1007L);
    when(this.thread.getKernelTime()).thenReturn(100L, 102L);

    ResourceUsageFeatureExplainer explainer = new ResourceUsageFeatureExplainer(this.systemInfo);
    SearchExplainInformationBuilder first = SearchExplainInformationBuilder.newBuilder();
    SearchExplainInformationBuilder second = SearchExplainInformationBuilder.newBuilder();

    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, first);
    var firstExplain = first.build();

    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, second);
    var secondExplain = second.build();

    Truth.assertThat(firstExplain.resourceUsage().get().maxReportingThreads())
        .isEqualTo(secondExplain.resourceUsage().get().maxReportingThreads());
  }

  @Test
  public void testDoNotEmitForQueryPlanner() {
    ResourceUsageFeatureExplainer explainer = new ResourceUsageFeatureExplainer(new SystemInfo());
    SearchExplainInformationBuilder builder = spy(new SearchExplainInformationBuilder());

    explainer.emitExplanation(Explain.Verbosity.QUERY_PLANNER, builder);
    verify(builder, never()).resourceUsage(any());
  }
}
