package com.xgen.mongot.index.lucene.explain.explainers;

import com.google.common.base.Ticker;
import com.google.common.testing.FakeTicker;
import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class CollectorTimingFeatureExplainerTest {
  private static Ticker fakeTicker() {
    return new FakeTicker().setAutoIncrementStep(Duration.ofMillis(1));
  }

  @Test
  public void testOneTiming() {
    var builder = SearchExplainInformationBuilder.newBuilder();
    var explainer = new CollectorTimingFeatureExplainer(fakeTicker());
    explainer.getAllCollectorTimings().split(ExplainTimings.Type.COLLECT).close();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);
    var output = builder.build().collectStats().get().allCollectorStats();
    Assert.assertTrue(output.isPresent());
    Truth.assertThat(output.get().millisElapsed()).isWithin(0.01).of(1);
    Assert.assertEquals(Optional.of(Map.of("collect", 1L)), output.get().invocationCounts());
  }

  @Test
  public void testMultipleTimings() {
    var builder = SearchExplainInformationBuilder.newBuilder();
    var explainer = new CollectorTimingFeatureExplainer(fakeTicker());
    explainer.getAllCollectorTimings().split(ExplainTimings.Type.COLLECT).close();
    explainer.getAllCollectorTimings().split(ExplainTimings.Type.SET_SCORER).close();
    explainer.getAllCollectorTimings().split(ExplainTimings.Type.COLLECT).close();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);
    var output = builder.build().collectStats().get().allCollectorStats();
    Assert.assertTrue(output.isPresent());
    Truth.assertThat(output.get().millisElapsed()).isWithin(0.01).of(3);
    Assert.assertEquals(
        Optional.of(Map.of("collect", 2L, "setScorer", 1L)), output.get().invocationCounts());
  }
}
