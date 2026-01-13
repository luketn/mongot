package com.xgen.mongot.index.lucene.explain.explainers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;

/** Maintains timing stats for explaining result collection. */
public class CollectorTimingFeatureExplainer implements FeatureExplainer {
  private final ExplainTimings allCollectorTimings;
  private final ExplainTimings facetCollectorTimings;

  public CollectorTimingFeatureExplainer() {
    this.allCollectorTimings = ExplainTimings.builder().build();
    this.facetCollectorTimings = ExplainTimings.builder().build();
  }

  @VisibleForTesting
  CollectorTimingFeatureExplainer(Ticker ticker) {
    this.allCollectorTimings = ExplainTimings.builder().withTicker(ticker).build();
    this.facetCollectorTimings = ExplainTimings.builder().withTicker(ticker).build();
  }

  public ExplainTimings getAllCollectorTimings() {
    return this.allCollectorTimings;
  }

  public ExplainTimings getFacetCollectorTimings() {
    return this.facetCollectorTimings;
  }

  @Override
  public synchronized void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    if (verbosity.equals(Explain.Verbosity.QUERY_PLANNER)) {
      return;
    }

    if (!this.allCollectorTimings.allTimingDataIsEmpty()) {
      builder.allCollectorStats(
          QueryExecutionArea.collectAreaFor(this.allCollectorTimings.extractTimingData()));
    }

    if (!this.facetCollectorTimings.allTimingDataIsEmpty()) {
      builder.facetCollectorStats(
          QueryExecutionArea.collectAreaFor(this.facetCollectorTimings.extractTimingData()));
    }
  }
}
