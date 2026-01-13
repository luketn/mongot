package com.xgen.mongot.index.lucene.facet;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.Check;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexReader;

/**
 * This class is a cache for ordinal maps on `token` fields. Ordinal maps are used in facet queries,
 * and we calculate ordinal maps upon the initial facet query on a field, then cache the ordinal
 * maps.
 */
public class TokenFacetsStateCache {
  private static final Duration DEFAULT_FACET_CACHE_EXPIRY = Duration.ofDays(1);
  private final Ticker ticker;
  private final Expiry<String, Optional<TokenSsdvFacetState>> expiry;
  private final LoadingCache<String, Optional<TokenSsdvFacetState>> cache;
  private final Optional<Integer> cardinalityLimit;

  private TokenFacetsStateCache(
      LoadingCache<String, Optional<TokenSsdvFacetState>> cache,
      Expiry<String, Optional<TokenSsdvFacetState>> expiry,
      Optional<Integer> cardinalityLimit,
      Ticker ticker) {
    this.ticker = ticker;
    this.cache = cache;
    this.expiry = expiry;
    this.cardinalityLimit = cardinalityLimit;
  }

  /**
   * Default method of creating this cache. Given a reader, use system ticker and default entry
   * expiration time.
   *
   * @param reader Lucene Index Reader
   * @return instance of TokenFacetsStateCache, tied to the IndexReader
   */
  public static TokenFacetsStateCache create(
      IndexReader reader, Optional<Integer> cardinalityLimit) {
    return create(reader, DEFAULT_FACET_CACHE_EXPIRY, cardinalityLimit, Ticker.systemTicker());
  }

  @VisibleForTesting
  static TokenFacetsStateCache create(
      IndexReader reader,
      Duration expireDuration,
      Optional<Integer> cardinalityLimit,
      Ticker ticker) {
    var expiry = new CustomExpiry(expireDuration);
    return new TokenFacetsStateCache(
        Caffeine.newBuilder()
            .softValues()
            .ticker(ticker)
            .expireAfter(expiry)
            .build(new FacetsCacheLoader(reader, cardinalityLimit)),
        expiry,
        cardinalityLimit,
        ticker);
  }

  private record FacetsCacheLoader(IndexReader indexReader, Optional<Integer> cardinalityLimit)
      implements CacheLoader<String, Optional<TokenSsdvFacetState>> {

    /**
     * Loads the corresponding TokenSsdvFacetState for a lucene field into the cache
     *
     * @param luceneFieldName the lucene field name for which an ordinal map will be created and
     *     cached
     * @return TokenSsdvFacetState
     * @throws IOException if accessing DocValues fails in an unexpected way
     * @throws TokenFacetsCardinalityLimitExceededException if the cardinality of the field is above
     *     the limit
     */
    @Override
    public Optional<TokenSsdvFacetState> load(String luceneFieldName)
        throws IOException, TokenFacetsCardinalityLimitExceededException {
      return TokenSsdvFacetState.create(this.indexReader, luceneFieldName, this.cardinalityLimit);
    }
  }

  /**
   * Grabs an entry from the cache if it is loaded, otherwise creates and loads the entry. Cache
   * contains soft references, which could have been garbage-collected. Re-compute the ordinal map
   * in that case. Existing references will have their `lastQueriedTime` updated.
   *
   * @param luceneFieldName the lucene path to the field requested for faceting
   * @return {@link SortedSetDocValuesReaderState} ordinal map entry. Should not be Optional.empty()
   *     since we re-build the ordinal map if evicted, but programmatically possible so we return an
   *     Optional.
   * @throws TokenFacetsCardinalityLimitExceededException if cardinality limit is exceeded
   * @throws RuntimeException or Error if the {@link CacheLoader} does so, in which case the mapping
   *     is left unestablished.
   */
  public Optional<TokenSsdvFacetState> get(String luceneFieldName)
      throws TokenFacetsCardinalityLimitExceededException {
    try {
      return this.cache.get(luceneFieldName);
    } catch (CompletionException e) {
      if (e.getCause()
          instanceof
          TokenFacetsCardinalityLimitExceededException cardinalityLimitExceededException) {
        throw cardinalityLimitExceededException;
      }
      // other exception types are not expected since
      // TokenFacetsCardinalityLimitExceededException is the only CheckedException thrown in `load`
      throw e;
    }
  }

  @VisibleForTesting
  public Optional<Duration> getTimeBeforeExpiring(String luceneFieldName) {
    return this.cache
        .policy()
        .expireVariably()
        .flatMap(policy -> policy.getExpiresAfter(luceneFieldName));
  }

  /**
   * Clones the cache for a new reader. Evicts any expired entries from the old map, and uses the
   * new index reader but the field names from the old map to rebuild the cache (Note: does not use
   * the old ordinal maps since those are tied to the old index reader).
   *
   * @param newReader new IndexReader
   * @return {@link TokenFacetsStateCache} refreshed cache tied to new index reader
   */
  public TokenFacetsStateCache cloneWithNewIndexReader(IndexReader newReader) throws IOException {
    var newCache =
        Caffeine.newBuilder()
            .ticker(this.ticker)
            .softValues()
            .expireAfter(this.expiry)
            .build(new FacetsCacheLoader(newReader, this.cardinalityLimit));

    for (Map.Entry<String, Optional<TokenSsdvFacetState>> entry : this.cache.asMap().entrySet()) {
      String key = entry.getKey();
      Optional<TokenSsdvFacetState> value = entry.getValue();
      // Each cache is created with a variable expiration policy so this should always be present.
      var expirePolicy = Check.isPresent(this.cache.policy().expireVariably(), "expirePolicy");
      Optional<Duration> expiresAfter = expirePolicy.getExpiresAfter(key);
      if (expiresAfter.isEmpty()) {
        // will return empty if entry is expired
        continue;
      }
      // should not be possible to be negative but just to be safe
      if (expiresAfter.get().isPositive()) {
        var newCacheExpirePolicy =
            Check.isPresent(newCache.policy().expireVariably(), "expirePolicy");
        if (value.isPresent()) {
          try {
            newCacheExpirePolicy.put(
                key,
                TokenSsdvFacetState.create(
                    newReader, value.get().luceneFieldName, this.cardinalityLimit),
                expiresAfter.get());
          } catch (TokenFacetsCardinalityLimitExceededException exception) {
            // If cardinality limit for a field is exceeded when
            // refreshing cache, we can simply exclude it from the new cache.
          }
        }
      }
    }
    return new TokenFacetsStateCache(newCache, this.expiry, this.cardinalityLimit, this.ticker);
  }

  @VisibleForTesting
  Map<String, Optional<TokenSsdvFacetState>> asMap() {
    return this.cache.asMap();
  }

  // Need a variable expiration class to enable modifying
  private static class CustomExpiry implements Expiry<String, Optional<TokenSsdvFacetState>> {
    final long expiry;

    private CustomExpiry(Duration expiry) {
      this.expiry = expiry.toNanos();
    }

    @Override
    public long expireAfterCreate(
        String key, Optional<TokenSsdvFacetState> value, long currentTime) {
      return this.expiry;
    }

    @Override
    public long expireAfterUpdate(
        String key, Optional<TokenSsdvFacetState> value, long currentTime, long currentDuration) {
      return this.expiry;
    }

    @Override
    public long expireAfterRead(
        String key, Optional<TokenSsdvFacetState> value, long currentTime, long currentDuration) {
      return this.expiry;
    }
  }
}
