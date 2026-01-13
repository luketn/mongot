package com.xgen.mongot.index.analyzer.attributes;

/**
 * A TokenStreamTypeAware instance is aware of whether or not it produces graph- or stream- type
 * token streams.
 */
public interface TokenStreamTypeAware {
  /**
   * Returns the {@link TokenStreamType} that this instance produces. Most analyzers produce {@code
   * TokenStreamType.STREAM} token streams - <a
   * href="http://blog.mikemccandless.com/2012/04/lucenes-tokenstreams-are-actually.html">this blog
   * post</a> describes more about what it means for an analyzer to produce graph token streams.
   */
  TokenStreamType getTokenStreamType();

  interface Stream extends TokenStreamTypeAware {
    default TokenStreamType getTokenStreamType() {
      return TokenStreamType.STREAM;
    }
  }

  interface Graph extends TokenStreamTypeAware {
    default TokenStreamType getTokenStreamType() {
      return TokenStreamType.GRAPH;
    }
  }
}
