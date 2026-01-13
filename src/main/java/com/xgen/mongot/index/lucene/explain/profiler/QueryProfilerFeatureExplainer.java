package com.xgen.mongot.index.lucene.explain.profiler;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryNodeException;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryProfilerFeatureExplainer implements FeatureExplainer {
  private final QueryProfiler profiler;
  private static final Logger LOG = LoggerFactory.getLogger(QueryProfilerFeatureExplainer.class);
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;

  public QueryProfilerFeatureExplainer(IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {
    this(new QueryProfiler(), metricsUpdater);
  }

  @VisibleForTesting
  QueryProfilerFeatureExplainer(
      QueryProfiler profiler, IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {
    this.profiler = profiler;
    this.metricsUpdater = metricsUpdater;
  }

  public QueryProfiler getQueryProfiler() {
    return this.profiler;
  }

  @Override
  public void aggregate() {
    try {
      this.profiler.aggregate();
    } catch (RewrittenQueryNodeException e) {
      LOG.warn("Exception caught when aggregating query stats across batches:", e);
      this.metricsUpdater.getFailedExplainQueryAggregate().increment();
    }
  }

  @Override
  public void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    var allExplainInfos = this.profiler.explainInformation(verbosity);
    if (verbosity.equals(Explain.Verbosity.ALL_PLANS_EXECUTION)) {
      builder.queryExplainInfos(allExplainInfos);
      return;
    }

    int startIndex = Math.max(0, allExplainInfos.size() - 10);
    builder.queryExplainInfos(allExplainInfos.subList(startIndex, allExplainInfos.size()));
  }
}
