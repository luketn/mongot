package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class VectorIndexDefinitionGenerationBuilder {

  private Optional<VectorIndexDefinition> definition = Optional.empty();
  private Optional<Generation> generation = Optional.empty();

  public static VectorIndexDefinitionGenerationBuilder builder() {
    return new VectorIndexDefinitionGenerationBuilder();
  }

  public VectorIndexDefinitionGenerationBuilder definition(VectorIndexDefinition definition) {
    this.definition = Optional.of(definition);
    return this;
  }

  public VectorIndexDefinitionGenerationBuilder generation(int user, int indexFormat) {
    return generation(
        new Generation(new UserIndexVersion(user), IndexFormatVersion.create(indexFormat)));
  }

  public VectorIndexDefinitionGenerationBuilder generation(Generation generation) {
    this.generation = Optional.of(generation);
    return this;
  }

  public VectorIndexDefinitionGeneration build() {
    return new VectorIndexDefinitionGeneration(
        Check.isPresent(this.definition, "definition"),
        Check.isPresent(this.generation, "generation"));
  }
}
