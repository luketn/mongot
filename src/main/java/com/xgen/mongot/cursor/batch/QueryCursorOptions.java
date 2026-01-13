package com.xgen.mongot.cursor.batch;

import java.util.Objects;
import java.util.Optional;

/** These are server-supplied options provided when a cursor is first established. */
public class QueryCursorOptions extends BatchCursorOptions {

  private static final QueryCursorOptions DEFAULT_OPTIONS =
      new QueryCursorOptions(Optional.empty(), Optional.empty(), false);
  private final boolean requireSequenceTokens;

  public QueryCursorOptions(
      Optional<Integer> docsRequested, Optional<Integer> batchSize, boolean requireSequenceTokens) {
    super(docsRequested, batchSize);
    this.requireSequenceTokens = requireSequenceTokens;
  }

  /** Default cursor option values if none are provided by the server. */
  public static QueryCursorOptions empty() {
    return DEFAULT_OPTIONS;
  }

  /**
   * True if a downstream stage relies on 'searchSequenceTokens` being provided in {@link
   * com.xgen.mongot.index.SearchResult}.
   */
  public boolean requireSequenceTokens() {
    return this.requireSequenceTokens;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryCursorOptions that = (QueryCursorOptions) o;
    return this.requireSequenceTokens == that.requireSequenceTokens && super.equals(that);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.requireSequenceTokens, super.hashCode());
  }
}
