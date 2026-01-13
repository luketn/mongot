package com.xgen.mongot.index.lucene.explain.explainers;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import org.junit.Test;

public class ResultMaterializationFeatureExplainerTest {
  @Test
  public void testTimings() throws InterruptedException {
    var explainer = new ResultMaterializationFeatureExplainer();
    var retrieveAndSerialize =
        explainer.getTimings().split(ExplainTimings.Type.RETRIEVE_AND_SERIALIZE);
    Thread.sleep(10);
    retrieveAndSerialize.close();
    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);

    var invocations = builder.resultMaterializationStats.get().stats().invocationCounts().get();
    Truth.assertThat(invocations).hasSize(1);
    Truth.assertThat(invocations)
        .containsExactly(ExplainTimings.Type.RETRIEVE_AND_SERIALIZE.getName(), 1L);
  }

  @Test
  public void testTimingsQueryPlanner() throws InterruptedException {
    var explainer = new ResultMaterializationFeatureExplainer();
    var retrieveAndSerialize =
        explainer.getTimings().split(ExplainTimings.Type.RETRIEVE_AND_SERIALIZE);
    Thread.sleep(10);
    retrieveAndSerialize.close();
    var builder = new SearchExplainInformationBuilder();
    explainer.emitExplanation(Explain.Verbosity.QUERY_PLANNER, builder);

    Truth.assertThat(builder.resultMaterializationStats).isEmpty();
  }
}
