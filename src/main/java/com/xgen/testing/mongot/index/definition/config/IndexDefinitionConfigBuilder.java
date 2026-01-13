package com.xgen.testing.mongot.index.definition.config;

import com.xgen.mongot.index.definition.config.IndexDefinitionConfig;
import java.util.Optional;

public class IndexDefinitionConfigBuilder {

  private Optional<Integer> maxEmbeddedDocumentsNestingLevel = Optional.empty();

  private IndexDefinitionConfigBuilder() {}

  public static IndexDefinitionConfigBuilder builder() {
    return new IndexDefinitionConfigBuilder();
  }

  public IndexDefinitionConfigBuilder maxEmbeddedDocumentsNestingLevel(int nestingLevel) {
    this.maxEmbeddedDocumentsNestingLevel = Optional.of(nestingLevel);
    return this;
  }

  public IndexDefinitionConfig build() {
    return new IndexDefinitionConfig(this.maxEmbeddedDocumentsNestingLevel);
  }
}
