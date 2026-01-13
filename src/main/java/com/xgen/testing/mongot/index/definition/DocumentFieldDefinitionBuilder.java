package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IllegalEmbeddedFieldException;
import com.xgen.mongot.util.Check;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;

public class DocumentFieldDefinitionBuilder {

  private Optional<DynamicDefinition> dynamic = Optional.empty();
  private final Map<String, FieldDefinition> fields = new HashMap<>();

  public static DocumentFieldDefinitionBuilder builder() {
    return new DocumentFieldDefinitionBuilder();
  }

  public DocumentFieldDefinitionBuilder dynamic(boolean dynamic) {
    this.dynamic = Optional.of(new DynamicDefinition.Boolean(dynamic));
    return this;
  }

  public DocumentFieldDefinitionBuilder dynamic(String typeSet) {
    this.dynamic = Optional.of(new DynamicDefinition.Document(typeSet));
    return this;
  }

  public DocumentFieldDefinitionBuilder dynamic(DynamicDefinition dynamicDefinition) {
    this.dynamic = Optional.of(dynamicDefinition);
    return this;
  }

  public DocumentFieldDefinitionBuilder field(String name, FieldDefinition definition) {
    this.fields.put(name, definition);
    return this;
  }

  public DocumentFieldDefinition build() {
    try {
      return DocumentFieldDefinition.create(
          this.dynamic.orElse(DocumentFieldDefinition.Fields.DYNAMIC.getDefaultValue()),
          this.fields);
    } catch (IllegalEmbeddedFieldException e) {
      Assert.fail(e.getMessage());
    }
    return Check.unreachable();
  }
}
