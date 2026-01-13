package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.DateFacetFieldDefinition;

public class DateFacetFieldDefinitionBuilder {

  public static DateFacetFieldDefinitionBuilder builder() {
    return new DateFacetFieldDefinitionBuilder();
  }

  public DateFacetFieldDefinition build() {
    return new DateFacetFieldDefinition();
  }
}
