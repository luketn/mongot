package com.xgen.mongot.index.lucene.explain.explainers;

import com.xgen.mongot.index.lucene.explain.information.ResultMaterializationStats;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;

public class ResultMaterializationFeatureExplainer implements FeatureExplainer {
  private final ExplainTimings timings;

  public ResultMaterializationFeatureExplainer() {
    this.timings = ExplainTimings.builder().build();
  }

  public ExplainTimings getTimings() {
    return this.timings;
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    if (verbosity.equals(Explain.Verbosity.QUERY_PLANNER)) {
      return;
    }

    builder.resultMaterializationStats(
        new ResultMaterializationStats(
            QueryExecutionArea.resultMaterializationAreaFor(this.timings.extractTimingData())));
  }
}
