package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.server.command.search.definition.request.OptimizationFlagsDefinition;

public class OptimizationFlagsDefinitionBuilder {
  private Boolean omitSearchDocumentResults = false;

  public static OptimizationFlagsDefinitionBuilder builder() {
    return new OptimizationFlagsDefinitionBuilder();
  }

  public OptimizationFlagsDefinitionBuilder omitSearchDocumentResults(
      boolean omitSearchDocumentResults) {
    this.omitSearchDocumentResults = omitSearchDocumentResults;
    return this;
  }

  public OptimizationFlagsDefinition build() {
    return new OptimizationFlagsDefinition(this.omitSearchDocumentResults);
  }
}
