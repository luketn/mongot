package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.GeoFieldDefinition;
import java.util.Optional;

public class GeoFieldDefinitionBuilder {

  private Optional<Boolean> indexShapes = Optional.empty();

  public static GeoFieldDefinitionBuilder builder() {
    return new GeoFieldDefinitionBuilder();
  }

  public GeoFieldDefinitionBuilder indexShapes(boolean indexShapes) {
    this.indexShapes = Optional.of(indexShapes);
    return this;
  }

  public GeoFieldDefinition build() {
    return new GeoFieldDefinition(
        this.indexShapes.orElse(GeoFieldDefinition.Fields.INDEX_SHAPES.getDefaultValue()));
  }
}
