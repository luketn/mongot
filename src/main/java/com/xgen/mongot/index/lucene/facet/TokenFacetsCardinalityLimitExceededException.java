package com.xgen.mongot.index.lucene.facet;

import com.xgen.mongot.index.query.InvalidQueryException;

public class TokenFacetsCardinalityLimitExceededException extends InvalidQueryException {
  public TokenFacetsCardinalityLimitExceededException(String message) {
    super(message);
  }
}
