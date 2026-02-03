package com.xgen.mongot.index.lucene.query.custom;

import com.xgen.mongot.index.IndexMetricsUpdater;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

/**
 * A specialized implementation of Lucene's KnnFloatVectorQuery to integrate with Mongot's metrics
 * and serve as a potential extension point for future optimizations.
 */
public class MongotKnnFloatQuery extends KnnFloatVectorQuery {

  private final IndexMetricsUpdater.QueryingMetricsUpdater metrics;

  /** A convenience overload for creating an unfiltered KNN query. */
  public MongotKnnFloatQuery(
      IndexMetricsUpdater.QueryingMetricsUpdater metrics, String field, float[] target, int k) {
    this(metrics, field, target, k, null);
  }

  /**
   * Find the k nearest documents to the target vector according to the vectors in the given field.
   * target vector.
   *
   * @param metrics - a metrics updater that holds counters relating to query execution.
   * @param field – the lucene field name that has been indexed as a KnnFloatVectorField.
   * @param target – the target query vector
   * @param k – the number of documents to find
   * @param filter – an optional filter applied before the vector search, or null if the search is
   *     unfiltered.
   * @throws IllegalArgumentException – if k is less than 1
   */
  public MongotKnnFloatQuery(
      IndexMetricsUpdater.QueryingMetricsUpdater metrics,
      String field,
      float[] target,
      int k,
      @Nullable Query filter) {
    super(field, target, k, filter);
    this.metrics = metrics;
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      @Nullable Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    // Note: visitedLimit = acceptDocs.cardinality() + 1 if using filtered search, otherwise is
    // Integer.MAX_VALUE.
    // acceptDocs is null if there is no filter AND there are no deleted docs in the segment
    TopDocs result =
        super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager);
    var searchMode =
        result.totalHits.relation == TotalHits.Relation.EQUAL_TO
            ? IndexMetricsUpdater.KnnSearchMode.APPROXIMATE
            : IndexMetricsUpdater.KnnSearchMode.FALLBACK_TO_EXACT;

    this.metrics.incrementKnnSearchMode(searchMode);

    return result;
  }
}
