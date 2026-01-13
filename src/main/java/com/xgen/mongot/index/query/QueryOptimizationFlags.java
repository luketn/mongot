package com.xgen.mongot.index.query;

public record QueryOptimizationFlags(boolean omitSearchDocumentResults) {
  public static final QueryOptimizationFlags DEFAULT_OPTIONS = new QueryOptimizationFlags(false);
}
