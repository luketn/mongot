package com.xgen.mongot.index.lucene.util;

import com.xgen.mongot.index.definition.StringFieldDefinition;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public class FieldTypeBuilder {

  private final FieldType fieldType = new FieldType();

  public FieldTypeBuilder withIndexOptions(IndexOptions value) {
    this.fieldType.setIndexOptions(value);
    return this;
  }

  public FieldTypeBuilder withIndexOptions(StringFieldDefinition.IndexOptions value) {
    this.fieldType.setIndexOptions(convertIndexOptions(value));
    return this;
  }

  public FieldTypeBuilder tokenized(boolean value) {
    this.fieldType.setTokenized(value);
    return this;
  }

  public FieldTypeBuilder stored(boolean value) {
    this.fieldType.setStored(value);
    return this;
  }

  public FieldTypeBuilder omitNorms(boolean value) {
    this.fieldType.setOmitNorms(value);
    return this;
  }

  public FieldType build() {
    this.fieldType.freeze();
    return this.fieldType;
  }

  private IndexOptions convertIndexOptions(
      StringFieldDefinition.IndexOptions fieldDefinitionIndexOptions) {
    return switch (fieldDefinitionIndexOptions) {
      case DOCS -> IndexOptions.DOCS;
      case FREQS -> IndexOptions.DOCS_AND_FREQS;
      case POSITIONS -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
      case OFFSETS -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
    };
  }
}
