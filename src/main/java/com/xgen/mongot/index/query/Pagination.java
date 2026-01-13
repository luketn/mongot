package com.xgen.mongot.index.query;

import com.xgen.mongot.index.query.sort.SequenceToken;

/** Container for pagination parameters of {@link SearchQuery} */
public record Pagination(
    SequenceToken sequenceToken, com.xgen.mongot.index.query.Pagination.Type type) {

  public enum Type {
    /** Return results ordered after the specified sequence token. */
    SEARCH_AFTER,
    /** Reverses the sort order and returns results ordered before the sequence token */
    SEARCH_BEFORE
  }
}
