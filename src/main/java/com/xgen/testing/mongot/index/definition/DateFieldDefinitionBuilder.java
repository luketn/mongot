package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.DateFieldDefinition;

public class DateFieldDefinitionBuilder {

  public static DateFieldDefinitionBuilder builder() {
    return new DateFieldDefinitionBuilder();
  }

  public DateFieldDefinition build() {
    return new DateFieldDefinition();
  }
}
