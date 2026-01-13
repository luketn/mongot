package com.xgen.mongot.index.lucene.explain.knn;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.Bits;

/**
 * A decorator over KnnByteVectorQuery to intercept calls to 'approximateSearch' and 'exactSearch'
 * for instrumentation purposes.
 */
public class InstrumentableKnnByteVectorQuery extends KnnByteVectorQuery {
  private final KnnInstrumentationHelper instrumentationHelper;

  public InstrumentableKnnByteVectorQuery(
      KnnInstrumentationHelper instrumentationHelper, String field, byte[] target, int k) {
    super(field, target, k);
    this.instrumentationHelper = instrumentationHelper;
  }

  public InstrumentableKnnByteVectorQuery(
      KnnInstrumentationHelper instrumentationHelper,
      String field,
      byte[] target,
      int k,
      Query filter) {
    super(field, target, k, filter);
    this.instrumentationHelper = instrumentationHelper;
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return new InstrumentedTopKnnCollectorManager(k, searcher, this.instrumentationHelper);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {

    return this.instrumentationHelper.meteredApproximateSearch(
        context,
        acceptDocs,
        () -> super.approximateSearch(context, acceptDocs, visitedLimit, knnCollectorManager));
  }

  @Override
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {

    return this.instrumentationHelper.meteredExactSearch(
        context, acceptIterator, () -> super.exactSearch(context, acceptIterator, queryTimeout));
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return this.instrumentationHelper.meteredMergeLeafResults(
        perLeafResults, () -> TopDocs.merge(this.k, perLeafResults));
  }
}
