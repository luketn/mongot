package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.UuidFieldDefinition;

public class UuidFieldDefinitionBuilder {

  public static UuidFieldDefinitionBuilder builder() {
    return new UuidFieldDefinitionBuilder();
  }

  public UuidFieldDefinition build() {
    return new UuidFieldDefinition();
  }
}
