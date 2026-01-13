package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.VectorIndexFilterFieldDefinition;
import com.xgen.mongot.util.FieldPath;

public class VectorFilterFieldDefinitionBuilder {
  public static VectorIndexFilterFieldDefinition atPath(String path) {
    return new VectorIndexFilterFieldDefinition(FieldPath.parse(path));
  }
}
