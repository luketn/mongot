package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.definition.SynonymSourceDefinition;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class SynonymMappingDefinitionBuilder {
  private Optional<String> name = Optional.empty();
  private Optional<SynonymSourceDefinition> synonymSourceDefinition = Optional.empty();
  private Optional<String> analyzer = Optional.empty();

  public static SynonymMappingDefinitionBuilder builder() {
    return new SynonymMappingDefinitionBuilder();
  }

  public SynonymMappingDefinitionBuilder name(String name) {
    this.name = Optional.of(name);
    return this;
  }

  public SynonymMappingDefinitionBuilder synonymSourceDefinition(String collectionName) {
    this.synonymSourceDefinition = Optional.of(new SynonymSourceDefinition(collectionName));
    return this;
  }

  public SynonymMappingDefinitionBuilder analyzer(String analyzer) {
    this.analyzer = Optional.of(analyzer);
    return this;
  }

  public SynonymMappingDefinition build() {
    return new SynonymMappingDefinition(
        Check.isPresent(this.name, "name"),
        Check.isPresent(this.synonymSourceDefinition, "synonymSourceDefinition"),
        Check.isPresent(this.analyzer, "analyzer"));
  }

  public List<SynonymMappingDefinition> buildAsList() {
    return List.of(build());
  }
}
