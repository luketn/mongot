package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.ObjectIdFieldDefinition;

public class ObjectIdFieldDefinitionBuilder {

  public static ObjectIdFieldDefinitionBuilder builder() {
    return new ObjectIdFieldDefinitionBuilder();
  }

  public ObjectIdFieldDefinition build() {
    return new ObjectIdFieldDefinition();
  }
}
