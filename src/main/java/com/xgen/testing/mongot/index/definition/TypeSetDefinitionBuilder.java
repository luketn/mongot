package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TypeSetDefinitionBuilder {
  private Optional<String> name = Optional.empty();
  private final List<FieldTypeDefinition> types = new ArrayList<>();

  public static TypeSetDefinitionBuilder builder() {
    return new TypeSetDefinitionBuilder();
  }

  public TypeSetDefinitionBuilder name(String name) {
    this.name = Optional.of(name);
    return this;
  }

  public TypeSetDefinitionBuilder addType(FieldTypeDefinition type) {
    this.types.add(type);
    return this;
  }

  public TypeSetDefinition build() {
    Check.checkState(!this.types.isEmpty(), "at least one field type must be defined");
    return new TypeSetDefinition(Check.isPresent(this.name, "name"), this.types);
  }
}
