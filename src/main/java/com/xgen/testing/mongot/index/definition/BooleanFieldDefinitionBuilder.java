package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.BooleanFieldDefinition;

public class BooleanFieldDefinitionBuilder {

  public static BooleanFieldDefinitionBuilder builder() {
    return new BooleanFieldDefinitionBuilder();
  }

  public BooleanFieldDefinition build() {
    return new BooleanFieldDefinition();
  }
}
