package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.metrics.Timed;
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
    return Timed.<T, IOException, InvalidQueryException>supplier2(
        this.metricsUpdater.getLuceneTopDocsSearchLatencyTimer(),
        () -> this.searchManager.initialSearch(searcherReference, batchSize));
  }

  @Override
  public TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize)
      throws IOException {
    return Timed.supplier(
        this.metricsUpdater.getLuceneTopDocsSearchLatencyTimer(),
        () -> this.searchManager.getMoreTopDocs(searcherReference, lastScoreDoc, batchSize));
  }
}
