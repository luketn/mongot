package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.metrics.Timed;
import com.xgen.mongot.trace.Tracing;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import java.io.IOException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

/**
 * Measures time spent on the actual Lucene search (topDocs collection part), does not distinguish
 * between the initial search and subsequent searchAfter.
 */
public class MeteredLuceneSearchManager<T> implements LuceneSearchManager<T> {

  private final LuceneSearchManager<T> searchManager;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;

  public MeteredLuceneSearchManager(
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      LuceneSearchManager<T> searchManager) {
    this.searchManager = searchManager;
    this.metricsUpdater = metricsUpdater;
  }

  @Override
  public T initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException, InvalidQueryException {
    try (var span =
        Tracing.detailedSpanGuard(
            "mongot.lucene.collect_initial_top_docs",
            Attributes.builder().put("mongot.lucene.batch_size", batchSize).build())) {
      T result =
          Timed.<T, IOException, InvalidQueryException>supplier2(
              this.metricsUpdater.getLuceneTopDocsSearchLatencyTimer(),
              () -> this.searchManager.initialSearch(searcherReference, batchSize));
      if (result instanceof LuceneSearchManager.QueryInfo queryInfo) {
        setTopDocsAttributes(span.getSpan(), queryInfo.topDocs);
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
            "mongot.lucene.collect_more_top_docs",
            Attributes.builder().put("mongot.lucene.batch_size", batchSize).build())) {
      span.getSpan().setAttribute("mongot.lucene.search_after.doc", lastScoreDoc.doc);
      span.getSpan().setAttribute("mongot.lucene.search_after.score", lastScoreDoc.score);
      TopDocs topDocs =
          Timed.supplier(
              this.metricsUpdater.getLuceneTopDocsSearchLatencyTimer(),
              () -> this.searchManager.getMoreTopDocs(searcherReference, lastScoreDoc, batchSize));
      setTopDocsAttributes(span.getSpan(), topDocs);
      return topDocs;
    }
  }

  private static void setTopDocsAttributes(Span span, TopDocs topDocs) {
    span.setAttribute("mongot.lucene.top_docs.count", topDocs.scoreDocs.length);
    span.setAttribute("mongot.lucene.total_hits", topDocs.totalHits.value);
    span.setAttribute("mongot.lucene.total_hits.relation", topDocs.totalHits.relation.name());
    if (topDocs.scoreDocs.length > 0) {
      ScoreDoc first = topDocs.scoreDocs[0];
      ScoreDoc last = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
      span.setAttribute("mongot.lucene.first_hit.doc", first.doc);
      span.setAttribute("mongot.lucene.first_hit.score", first.score);
      span.setAttribute("mongot.lucene.last_hit.doc", last.doc);
      span.setAttribute("mongot.lucene.last_hit.score", last.score);
    }
  }
}
