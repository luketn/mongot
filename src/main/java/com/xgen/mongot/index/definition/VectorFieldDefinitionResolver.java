package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.util.Objects;
import java.util.Optional;

public class VectorFieldDefinitionResolver implements FieldDefinitionResolver {

  private final VectorIndexDefinition definition;
  private final IndexCapabilities indexCapabilities;

  public VectorFieldDefinitionResolver(
      VectorIndexDefinition definition, IndexFormatVersion indexFormatVersion) {
    this.definition = definition;
    this.indexCapabilities = definition.getIndexCapabilities(indexFormatVersion);
  }

  @Override
  public boolean isUsed(FieldPath path) {
    return this.definition.getFields().stream()
        .map(VectorIndexFieldDefinition::getPath)
        .anyMatch(configuredPath -> configuredPath.getPathHierarchy().contains(path));
  }

  public IndexCapabilities getIndexCapabilities() {
    return this.indexCapabilities;
  }

  public boolean isIndexed(FieldPath path, VectorIndexFieldDefinition.Type type) {
    return this.definition.getFields().stream()
        .filter(definition -> definition.getType() == type)
        .map(VectorIndexFieldDefinition::getPath)
        .anyMatch(configuredPath -> Objects.equals(configuredPath, path));
  }

  @Override
  public Optional<VectorFieldSpecification> getVectorFieldSpecification(FieldPath path) {
    return this.definition.getFields().stream()
        .filter(definition -> Objects.equals(definition.getPath(), path))
        .filter(VectorIndexFieldDefinition::isVectorField)
        .map(VectorIndexFieldDefinition::asVectorField)
        .map(VectorIndexVectorFieldDefinition::specification)
        .findFirst();
  }

  public ImmutableList<VectorIndexFieldDefinition> getFields() {
    return this.definition.getFields();
  }
}
