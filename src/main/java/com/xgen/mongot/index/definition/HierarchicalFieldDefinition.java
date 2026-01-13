package com.xgen.mongot.index.definition;

import java.util.Map;
import java.util.Optional;

public interface HierarchicalFieldDefinition {
  Optional<FieldDefinition> getField(String fieldName);

  Map<String, FieldDefinition> fields();

  DynamicDefinition dynamic();

  FieldHierarchyContext fieldHierarchyContext();
}
