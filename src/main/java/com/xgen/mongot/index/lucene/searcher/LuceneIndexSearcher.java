package com.xgen.mongot.index.lucene.searcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.Check;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.Similarity;

/**
 * {@link IndexSearcher} implementation, which encapsulates optional {@link #facetsState}. Facet
 * state is present only when the index definition contains facet fields and some documents with
 * non-empty facet fields were already indexed.
 */
public class LuceneIndexSearcher extends IndexSearcher {

  private final Optional<SortedSetDocValuesReaderState> facetsState;
  private final Optional<TokenFacetsStateCache> tokenFacetsStateCache;
  private final FieldToSortableTypesMapping fieldToSortableTypesMapping;

  @VisibleForTesting
  public static LuceneIndexSearcher create(
      IndexReader newReader,
      QueryCacheProvider queryCacheProvider,
      Optional<LuceneIndexSearcher> previousSearcher,
      Optional<Similarity> similarity,
      boolean stringFacetFieldIndexed,
      boolean enableFacetingOverTokenFields,
      Optional<Integer> cardinalityLimit)
      throws IOException {
    return create(
        newReader,
        queryCacheProvider,
        previousSearcher,
        similarity,
        stringFacetFieldIndexed,
        enableFacetingOverTokenFields,
        cardinalityLimit,
        Optional.empty());
  }

  static LuceneIndexSearcher create(
      IndexReader newReader,
      QueryCacheProvider queryCacheProvider,
      Optional<LuceneIndexSearcher> previousSearcher,
      Optional<Similarity> similarity,
      boolean stringFacetFieldIndexed,
      boolean enableFacetingOverTokenFields,
      Optional<Integer> cardinalityLimit,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater)
      throws IOException {
    return create(
        newReader,
        queryCacheProvider,
        previousSearcher,
        similarity,
        stringFacetFieldIndexed,
        enableFacetingOverTokenFields,
        cardinalityLimit,
        Optional.of(queryingMetricsUpdater.getTokenFacetsStateRefreshLatencyTimer()));
  }

  private static LuceneIndexSearcher create(
      IndexReader newReader,
      QueryCacheProvider queryCacheProvider,
      Optional<LuceneIndexSearcher> previousSearcher,
      Optional<Similarity> similarity,
      boolean stringFacetFieldIndexed,
      boolean enableFacetingOverTokenFields,
      Optional<Integer> cardinalityLimit,
      Optional<Timer> facetsRefreshTimer)
      throws IOException {
    LuceneIndexSearcher indexSearcher =
        new LuceneIndexSearcher(
            newReader,
            FieldToSortableTypesMapping.create(newReader),
            createTokenFacetsStateCache(
                newReader,
                previousSearcher,
                enableFacetingOverTokenFields,
                cardinalityLimit,
                facetsRefreshTimer),
            createFacetState(newReader, previousSearcher, stringFacetFieldIndexed));

    similarity.ifPresent(indexSearcher::setSimilarity);
    queryCacheProvider.queryCache().ifPresent(indexSearcher::setQueryCache);

    return indexSearcher;
  }

  private LuceneIndexSearcher(
      IndexReader newReader,
      FieldToSortableTypesMapping fieldToSortableTypesMapping,
      Optional<TokenFacetsStateCache> tokenFacetsStateCache,
      Optional<SortedSetDocValuesReaderState> facetsState) {
    super(newReader);
    this.fieldToSortableTypesMapping = fieldToSortableTypesMapping;
    this.facetsState = facetsState;
    this.tokenFacetsStateCache = tokenFacetsStateCache;
  }

  /**
   * Creates a new IndexSearcher, but re-uses IndexReader and FacetsState from the {@param other},
   * as only one IndexReader per index is optimal for performance reasons. To create a concurrent
   * Searcher, use the overloaded constructor below.
   */
  protected LuceneIndexSearcher(LuceneIndexSearcher other) {
    super(other.getIndexReader());
    setSimilarity(other.getSimilarity());
    setQueryCache(other.getQueryCache());
    this.facetsState = other.facetsState;
    this.fieldToSortableTypesMapping = other.fieldToSortableTypesMapping;
    this.tokenFacetsStateCache = other.tokenFacetsStateCache;
    Check.argIsNull(other.getExecutor(), "executor");
  }

  /**
   * Creates a new concurrent IndexSearcher, but re-uses IndexReader and FacetsState from the
   * {@param other}, as only one IndexReader per index is optimal for performance reasons.
   */
  protected LuceneIndexSearcher(LuceneIndexSearcher other, Executor executor) {
    super(other.getIndexReader(), executor);
    setSimilarity(other.getSimilarity());
    setQueryCache(other.getQueryCache());
    this.facetsState = other.facetsState;
    this.fieldToSortableTypesMapping = other.fieldToSortableTypesMapping;
    this.tokenFacetsStateCache = other.tokenFacetsStateCache;
    Check.argNotNull(executor, "executor");
  }

  private static Optional<TokenFacetsStateCache> createTokenFacetsStateCache(
      IndexReader reader,
      Optional<LuceneIndexSearcher> previousSearcher,
      boolean enableFacetingOverTokenFields,
      Optional<Integer> cardinalityLimit,
      Optional<Timer> refreshTimer)
      throws IOException {
    if (!enableFacetingOverTokenFields) {
      return Optional.empty();
    }

    // the cache optional should only be empty if the FF is off, so this case should not ever hit
    if (previousSearcher.flatMap(LuceneIndexSearcher::getTokenFacetsStateCache).isEmpty()) {
      return Optional.of(TokenFacetsStateCache.create(reader, cardinalityLimit));
    }
    var stopwatch = Stopwatch.createStarted();
    var newCache =
        Optional.of(
            previousSearcher
                .flatMap(LuceneIndexSearcher::getTokenFacetsStateCache)
                .get()
                .cloneWithNewIndexReader(reader));
    refreshTimer.ifPresent(timer -> timer.record(stopwatch.stop().elapsed()));
    return newCache;
  }

  private static Optional<SortedSetDocValuesReaderState> createFacetState(
      IndexReader reader, Optional<LuceneIndexSearcher> previousSearcher, boolean facetsEnabled)
      throws IOException {

    if (!facetsEnabled) {
      return Optional.empty();
    }

    /*
     * Fast way to check that facets were indexed (faster than pulling all SortedSetDocValues, which
     * is done during DefaultSortedSetDocValuesReaderState instantiation). If not checked,
     * instantiation will fail in case when no facets were indexed yet.
     */
    if (previousSearcher.map(LuceneIndexSearcher::getFacetsState).isEmpty()) {
      boolean containsFacets =
          reader.leaves().stream()
              .anyMatch(
                  leaf ->
                      leaf.reader()
                              .getFieldInfos()
                              .fieldInfo(FieldName.StaticField.FACET.getLuceneFieldName())
                          != null);
      if (!containsFacets) {
        return Optional.empty();
      }
    }

    /*
     * For performance reasons, we optimistically checked existence of facets fields
     * only if facetsState did not exist before. We should still expect exception in
     * case when facet fields existed, but were removed.
     */
    try {
      return Optional.of(new DefaultSortedSetDocValuesReaderState(reader));
    } catch (IllegalArgumentException e) {
      if (e.getMessage() != null
          && e.getMessage().contains("was not indexed with SortedSetDocValues")) {
        return Optional.empty();
      }
      throw e;
    }
  }

  /**
   * Returns fieldsToSortableTypes, which is in sync with corresponding {@link IndexReader}.
   * fieldToSortableTypes is a mapping of fieldPath -> the names of all sortable data types present
   * in the index.
   */
  public FieldToSortableTypesMapping getFieldToSortableTypesMapping() {
    return this.fieldToSortableTypesMapping;
  }

  /**
   * Returns facetsState, which is in sync with corresponding {@link IndexReader}. Note that this
   * method can return {@link Optional#empty()} when facets are configured for the index, but no
   * facet fields were indexed yet.
   */
  public Optional<SortedSetDocValuesReaderState> getFacetsState() {
    return this.facetsState;
  }

  public Optional<TokenFacetsStateCache> getTokenFacetsStateCache() {
    return this.tokenFacetsStateCache;
  }
}
