package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.IndexReader;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.VectorIndexReader;
import com.xgen.mongot.index.VectorSearchResult;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.bson.BsonArray;

/**
 * Takes in multiple LuceneVectorIndexReaders, sends the query to each individual reader and
 * aggregates the search results. This class is thread-safe, but concurrently calling query() and
 * close() on it may cause query() to fail, without crashing mongot or other damaging effects, since
 * some of the underlying readers may have been closed.
 */
public class MultiLuceneVectorIndexReader implements VectorIndexReader {
  private final List<LuceneVectorIndexReader> readers;
  private final IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater;

  MultiLuceneVectorIndexReader(
      List<LuceneVectorIndexReader> readers,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater) {
    Check.checkState(
        readers.size() >= 2,
        "There must be >= 2 underlying readers to construct MultiLuceneVectorIndexReader.");
    this.readers = readers;
    this.metricsUpdater = metricsUpdater;
  }

  // If this is slow, we can explore querying each underlying LuceneVectorIndexReader with a
  // lowered limit.
  @Override
  public BsonArray query(MaterializedVectorSearchQuery query)
      throws ReaderClosedException, IOException, InvalidQueryException {
    List<Iterator<VectorSearchResult>> vectorSearchResultIterators =
        new ArrayList<>(this.readers.size());
    for (int i = 0; i < this.readers.size(); i++) {
      try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
        var reader = this.readers.get(i);
        vectorSearchResultIterators.add(reader.queryResults(query).iterator());
      }
    }

    List<VectorSearchResult> mergeVectorSearchResult =
        mergeVectorSearchResults(vectorSearchResultIterators, query.limit());

    return LuceneVectorIndexReader.getBsonArray(mergeVectorSearchResult, this.metricsUpdater);
  }

  @Override
  public void refresh() throws IOException, ReaderClosedException {
    for (var reader : this.readers) {
      reader.refresh();
    }
  }

  @Override
  public void open() {
    for (var reader : this.readers) {
      reader.open();
    }
  }

  @Override
  public void close() {
    for (var reader : this.readers) {
      reader.close();
    }
  }

  @VisibleForTesting
  int numUnderlyingReaders() {
    return this.readers.size();
  }

  @Override
  public long getRequiredMemoryForVectorData() throws ReaderClosedException {
    // Sum required memory metric from all the readers
    return CheckedStream.from(this.readers)
        .mapAndCollectChecked(IndexReader::getRequiredMemoryForVectorData)
        .stream()
        .reduce(0L, Long::sum);
  }

  private List<VectorSearchResult> mergeVectorSearchResults(
      List<Iterator<VectorSearchResult>> vectorSearchResultIterators, int limit) {
    var merged =
        Iterators.mergeSorted(
            vectorSearchResultIterators, VectorSearchResult.VECTOR_SEARCH_RESULT_COMPARATOR);
    return Lists.newArrayList(Iterators.limit(merged, limit));
  }
}
