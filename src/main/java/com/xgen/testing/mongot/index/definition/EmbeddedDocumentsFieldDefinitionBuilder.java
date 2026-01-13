package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.IllegalEmbeddedFieldException;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.util.Check;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Assert;

public class EmbeddedDocumentsFieldDefinitionBuilder {
  private Optional<DynamicDefinition> dynamic = Optional.empty();
  private final Map<String, FieldDefinition> fields = new HashMap<>();
  private Optional<StoredSourceDefinition> storedSourceDefinition = Optional.empty();

  public static EmbeddedDocumentsFieldDefinitionBuilder builder() {
    return new EmbeddedDocumentsFieldDefinitionBuilder();
  }

  public EmbeddedDocumentsFieldDefinitionBuilder dynamic(boolean dynamic) {
    this.dynamic = Optional.of(new DynamicDefinition.Boolean(dynamic));
    return this;
  }

  public EmbeddedDocumentsFieldDefinitionBuilder dynamic(String typeSet) {
    this.dynamic = Optional.of(new DynamicDefinition.Document(typeSet));
    return this;
  }

  public EmbeddedDocumentsFieldDefinitionBuilder dynamic(DynamicDefinition dynamicDefinition) {
    this.dynamic = Optional.of(dynamicDefinition);
    return this;
  }

  public EmbeddedDocumentsFieldDefinitionBuilder field(String name, FieldDefinition definition) {
    this.fields.put(name, definition);
    return this;
  }

  public EmbeddedDocumentsFieldDefinitionBuilder storedSource(
      StoredSourceDefinition storedSourceDefinition) {
    this.storedSourceDefinition = Optional.of(storedSourceDefinition);
    return this;
  }

  public EmbeddedDocumentsFieldDefinition build() {
    try {
      return EmbeddedDocumentsFieldDefinition.create(
          this.dynamic.orElse(EmbeddedDocumentsFieldDefinition.Fields.DYNAMIC.getDefaultValue()),
          this.fields,
          this.storedSourceDefinition);
    } catch (IllegalEmbeddedFieldException e) {
      Assert.fail(e.getMessage());
    }
    return Check.unreachable();
  }
}
