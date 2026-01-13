package com.xgen.testing.mongot.index.query;

import com.xgen.mongot.index.query.QueryOptimizationFlags;

public class QueryOptimizationFlagsBuilder {
  private Boolean omitSearchDocumentResults = false;

  public static QueryOptimizationFlagsBuilder builder() {
    return new QueryOptimizationFlagsBuilder();
  }

  public QueryOptimizationFlagsBuilder omitSearchDocumentResults(
      boolean omitSearchDocumentResults) {
    this.omitSearchDocumentResults = omitSearchDocumentResults;
    return this;
  }

  public QueryOptimizationFlags build() {
    return new QueryOptimizationFlags(this.omitSearchDocumentResults);
  }
}
