package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.CountMergingBatchProducer;
import com.xgen.mongot.index.CountMetaBatchProducer;
import com.xgen.mongot.index.IndexReader;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.MeteredBatchProducer;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.FieldInfos;

/**
 * Takes in multiple LuceneSearchIndexReaders, and dispatch the query properly to each individual
 * reader. This class is thread-safe, but concurrently calling query() and close() on it may cause
 * query() to fail, without crashing mongot or other damaging effects, since some of the underlying
 * readers may have been closed.
 */
public class MultiLuceneSearchIndexReader implements SearchIndexReader {
  private final List<LuceneSearchIndexReader> readers;

  MultiLuceneSearchIndexReader(List<LuceneSearchIndexReader> readers) {
    Check.checkState(
        readers.size() >= 2,
        "There must be >= 2 underlying readers to construct MultiLuceneSearchIndexReader.");
    this.readers = readers;
  }

  @Override
  public SearchProducerAndMetaResults query(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    return switch (query) {
      case OperatorQuery operatorQuery ->
          vectorOrOperatorQuery(
              query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      case VectorSearchQuery vectorSearchQuery ->
          vectorOrOperatorQuery(
              query, queryCursorOptions, batchSizeStrategy, QueryOptimizationFlags.DEFAULT_OPTIONS);
      case CollectorQuery collectorQuery ->
          collectorQuery(
              collectorQuery, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
    };
  }

  private SearchProducerAndMetaResults vectorOrOperatorQuery(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    Check.checkState(
        query instanceof VectorSearchQuery || query instanceof OperatorQuery,
        "query here must be a vector or operator query");
    List<LuceneSearchBatchProducer> searchBatchProducers = new ArrayList<>(this.readers.size());
    List<MetaResults> metaResultsList = new ArrayList<>(this.readers.size());
    for (int i = 0; i < this.readers.size(); i++) {
      try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
        var reader = this.readers.get(i);

        // Vector search query or operator query doesn't have string facet at all, so we can just
        // issue query() here.
        var result =
            reader.query(query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
        // These type casting are ugly, but I didn't find a clean way to avoid it.
        if (result.searchBatchProducer instanceof MeteredBatchProducer meteredBatchProducer) {
          if (meteredBatchProducer.unwrap()
              instanceof LuceneSearchBatchProducer luceneSearchBatchProducer) {
            searchBatchProducers.add(luceneSearchBatchProducer);
          }
        }
        metaResultsList.add(result.metaResults);
      }
    }
    var searchMergingBatchProducer =
        searchBatchProducers.isEmpty()
            ? new EmptySearchBatchProducer()
            : new SearchMergingBatchProducer(searchBatchProducers);
    // Vector search query or operator query doesn't have facet at all.
    return new SearchProducerAndMetaResults(
        searchMergingBatchProducer, MetaResults.mergeCountResult(metaResultsList));
  }

  private SearchProducerAndMetaResults collectorQuery(
      CollectorQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    // If the query is a collector search query with string facet, we have to issue an
    // intermediateQuery() to get _all_ instead of top string facet buckets.
    SearchProducerAndMetaProducer result =
        intermediateQuery(query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
    if (!(result.metaBatchProducer instanceof FacetMergingBatchProducer)) {
      throw new IllegalStateException(
          "metaProducer from intermediateOperatorQuery() must be a "
              + "LuceneFacetCollectorMetaBatchProducer.");
    }
    MetaResults metaResults =
        ((FacetMergingBatchProducer) result.metaBatchProducer)
            .getMetaResultsAndClose(query.count().type());
    return new SearchProducerAndMetaResults(result.searchBatchProducer, metaResults);
  }

  @Override
  public SearchProducerAndMetaProducer intermediateQuery(
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    List<LuceneSearchBatchProducer> searchBatchProducers = new ArrayList<>(this.readers.size());
    List<LuceneFacetCollectorMetaBatchProducer> facetBatchProducers =
        new ArrayList<>(this.readers.size());
    List<CountMetaBatchProducer> countBatchProducers = new ArrayList<>(this.readers.size());
    for (int i = 0; i < this.readers.size(); i++) {
      try (var indexPartitionResourceManager = Explain.maybeEnterIndexPartitionQueryContext(i)) {
        var reader = this.readers.get(i);

        var result =
            reader.intermediateQuery(
                query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
        // These type casting are ugly, but I didn't find a clean way to avoid them.
        if (result.searchBatchProducer instanceof MeteredBatchProducer meteredBatchProducer) {
          if (meteredBatchProducer.unwrap()
              instanceof LuceneSearchBatchProducer luceneSearchBatchProducer) {
            searchBatchProducers.add(luceneSearchBatchProducer);
          }
        }
        if (result.metaBatchProducer
            instanceof LuceneFacetCollectorMetaBatchProducer facetMetaBatchProducer) {
          facetBatchProducers.add(facetMetaBatchProducer);
        } else if (result.metaBatchProducer
            instanceof CountMetaBatchProducer countMetaBatchProducer) {
          countBatchProducers.add(countMetaBatchProducer);
        } else {
          throw new IllegalStateException(
              String.format(
                  "Cannot handle result.metaBatchProducer's type %s",
                  result.metaBatchProducer.getClass()));
        }
      }
    }
    var searchMergingBatchProducer =
        searchBatchProducers.isEmpty()
            ? new EmptySearchBatchProducer()
            : new SearchMergingBatchProducer(searchBatchProducers);
    BatchProducer mergedMetaProducer;
    if (!facetBatchProducers.isEmpty()) {
      mergedMetaProducer = FacetMergingBatchProducer.create(facetBatchProducers);
    } else if (!countBatchProducers.isEmpty()) {
      mergedMetaProducer = new CountMergingBatchProducer(countBatchProducers);
    } else {
      return Check.unreachable(
          "Either facetBatchProducers or countBatchProducers must be non-empty");
    }
    return new SearchProducerAndMetaProducer(searchMergingBatchProducer, mergedMetaProducer);
  }

  @Override
  public long getNumEmbeddedRootDocuments() throws IOException, ReaderClosedException {
    @Var long sum = 0;
    for (var reader : this.readers) {
      sum += reader.getNumEmbeddedRootDocuments();
    }
    return sum;
  }

  @Override
  public List<FieldInfos> getFieldInfos() throws IOException, ReaderClosedException {
    List<FieldInfos> fieldInfos = new ArrayList<>();
    for (var reader : this.readers) {
      fieldInfos.addAll(reader.getFieldInfos());
    }

    return fieldInfos;
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
    return CheckedStream.from(this.readers)
        .mapAndCollectChecked(IndexReader::getRequiredMemoryForVectorData)
        .stream()
        .reduce(0L, Long::sum);
  }
}
