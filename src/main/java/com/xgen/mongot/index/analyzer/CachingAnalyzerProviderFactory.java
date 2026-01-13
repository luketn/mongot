package com.xgen.mongot.index.analyzer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import org.apache.lucene.analysis.Analyzer;

/**
 * Wrap an AnalyzerFactory with a cache, evicting elements from the cache after a period of no
 * access.
 *
 * <p>Elements of class R (the input to an analyzer factory, e.g. an AutocompleteAnalyzerDefinition)
 * should override {@link Object#equals(Object)} so the cache knows * what inputs create equivalent
 * outputs. Implementers of {@link Object#equals(Object)} should also implement {@link
 * Object#hashCode()} as good form, thought that is not a requirement of this class.
 */
class CachingAnalyzerProviderFactory {

  static <R> AnalyzerFactory<R> expiringAfterNoAccessPeriod(
      AnalyzerFactory<R> factory, Duration duration) {
    LoadingCache<R, Analyzer> cache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(duration)
            .build(CacheLoader.from(factory::getAnalyzer));

    return new CachingAnalyzerProvider<>(cache);
  }

  static class CachingAnalyzerProvider<R> implements AnalyzerFactory<R> {

    private final LoadingCache<R, Analyzer> cache;

    CachingAnalyzerProvider(LoadingCache<R, Analyzer> cache) {
      this.cache = cache;
    }

    @Override
    public Analyzer getAnalyzer(R definition) {
      // Use getUnchecked because the getAnalyzer method of AnalyzerFactory cannot throw any checked
      // exceptions.
      return this.cache.getUnchecked(definition);
    }
  }
}
