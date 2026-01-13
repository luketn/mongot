package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.StringFacetFieldDefinition;

public class StringFacetFieldDefinitionBuilder {

  public static StringFacetFieldDefinitionBuilder builder() {
    return new StringFacetFieldDefinitionBuilder();
  }

  public StringFacetFieldDefinition build() {
    return new StringFacetFieldDefinition();
  }
}
