package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.index.IndexMetricsUpdater.QueryingMetricsUpdater;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.trace.Tracing;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Measures time spent on the actual Lucene search (topDocs collection part) for the vectorSearch
 * and collects addition metrics
 */
public class MeteredLuceneVectorSearchManager<T> implements LuceneSearchManager<T> {

  private final LuceneSearchManager<T> searchManager;
  private final QueryingMetricsUpdater metricsUpdater;
  private final VectorSearchCriteria criteria;
  private final VectorQuantization fieldQuantization;

  public MeteredLuceneVectorSearchManager(
      QueryingMetricsUpdater metricsUpdater,
      LuceneSearchManager<T> searchManager,
      VectorSearchCriteria criteria,
      VectorQuantization fieldQuantization) {
    this.searchManager = searchManager;
    this.metricsUpdater = metricsUpdater;
    this.criteria = criteria;
    this.fieldQuantization = fieldQuantization;
  }

  @Override
  public T initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException, InvalidQueryException {

    if (this.criteria instanceof ApproximateVectorSearchCriteria approximateCriteria) {
      var metric =
          switch (this.fieldQuantization) {
            case VectorQuantization.NONE -> this.metricsUpdater.getNumCandidatesUnquantized();
            case VectorQuantization.SCALAR -> this.metricsUpdater.getNumCandidatesScalarQuantized();
            case VectorQuantization.BINARY -> this.metricsUpdater.getNumCandidatesBinaryQuantized();
          };
      metric.record(approximateCriteria.numCandidates());
    }
    this.metricsUpdater.getLimitPerQuery().record(this.criteria.limit());

    try (var span =
        Tracing.detailedSpanGuard(
            "mongot.lucene.vector_initial_top_docs",
            Attributes.builder()
                .put("mongot.lucene.batch_size", batchSize)
                .put("mongot.vector.search.type", this.criteria.getVectorSearchType().name())
                .put("mongot.vector.limit", this.criteria.limit())
                .put("mongot.vector.quantization", this.fieldQuantization.name())
                .build())) {
      T result =
          Timed.<T, IOException, InvalidQueryException>supplier2(
              this.metricsUpdater.getVectorSearchInitialTopDocsLatencyTimer(),
              () -> this.searchManager.initialSearch(searcherReference, batchSize));
      if (result instanceof LuceneSearchManager.QueryInfo queryInfo) {
        span.getSpan().setAttribute("mongot.lucene.top_docs.count", queryInfo.topDocs.scoreDocs.length);
        span.getSpan().setAttribute("mongot.lucene.total_hits", queryInfo.topDocs.totalHits.value);
        span.getSpan().setAttribute("mongot.lucene.exhausted", queryInfo.luceneExhausted);
      }
      return result;
    }
  }

  @Override
  public TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize)
      throws IOException {
    try (var span =
        Tracing.detailedSpanGuard(
            "mongot.lucene.vector_get_more_top_docs",
            Attributes.builder().put("mongot.lucene.batch_size", batchSize).build())) {
      TopDocs topDocs =
          Timed.supplier(
              this.metricsUpdater.getVectorSearchGetMoreTopDocsLatencyTimer(),
              () -> this.searchManager.getMoreTopDocs(searcherReference, lastScoreDoc, batchSize));
      span.getSpan().setAttribute("mongot.lucene.top_docs.count", topDocs.scoreDocs.length);
      span.getSpan().setAttribute("mongot.lucene.total_hits", topDocs.totalHits.value);
      return topDocs;
    }
  }
}
