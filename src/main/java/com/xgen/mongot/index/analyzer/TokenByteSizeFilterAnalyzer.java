package com.xgen.mongot.index.analyzer;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

/**
 * Wraps a given Analyzer with a TokenByteSizeFilter as the final stage. This ensures that we do not
 * pass Lucene any terms larger than IndexWriter.MAX_TERM_SIZE.
 */
public class TokenByteSizeFilterAnalyzer extends AnalyzerWrapper {

  private final Analyzer delegate;
  private final Optional<Integer> maxByteSize;

  public TokenByteSizeFilterAnalyzer(Analyzer delegate, int maxByteSize) {
    this(delegate, Optional.of(maxByteSize));
  }

  public TokenByteSizeFilterAnalyzer(Analyzer delegate) {
    this(delegate, Optional.empty());
  }

  private TokenByteSizeFilterAnalyzer(Analyzer delegate, Optional<Integer> maxByteSize) {
    super(delegate.getReuseStrategy());

    Check.argNotNull(delegate, "delegate");
    Check.argNotNull(maxByteSize, "maxByteSize");

    this.delegate = delegate;
    this.maxByteSize = maxByteSize;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return this.delegate;
  }

  @VisibleForTesting
  public String delegateAnalyzerName() {
    return this.delegate.getClass().getSimpleName();
  }

  @Override
  protected TokenStreamComponents wrapComponents(
      String fieldName, TokenStreamComponents components) {
    Check.argNotNull(fieldName, "fieldName");
    Check.argNotNull(components, "components");

    TokenByteSizeFilter filter =
        this.maxByteSize
            .map(m -> new TokenByteSizeFilter(components.getTokenStream(), fieldName, m))
            .orElseGet(() -> new TokenByteSizeFilter(components.getTokenStream(), fieldName));

    return new TokenStreamComponents(components.getSource(), filter);
  }
}
