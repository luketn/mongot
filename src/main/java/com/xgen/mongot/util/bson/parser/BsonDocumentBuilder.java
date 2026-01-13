package com.xgen.mongot.util.bson.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonElement;

public class BsonDocumentBuilder {

  private final List<BsonElement> elements;

  private BsonDocumentBuilder() {
    this.elements = new ArrayList<>();
  }

  public static BsonDocumentBuilder builder() {
    return new BsonDocumentBuilder();
  }

  public <T> BsonDocumentBuilder field(Field.Required<T> field, T value) {
    BsonElement element = new BsonElement(field.getName(), field.encode(value));
    this.elements.add(element);
    return this;
  }

  public <T> BsonDocumentBuilder field(Field.Optional<T> field, Optional<T> value) {
    if (value.isEmpty()) {
      return this;
    }

    BsonElement element = new BsonElement(field.getName(), field.encode(value));
    this.elements.add(element);
    return this;
  }

  public <T> BsonDocumentBuilder field(Field.WithDefault<T> field, T value) {
    BsonElement element = new BsonElement(field.getName(), field.encode(value));
    this.elements.add(element);
    return this;
  }

  public <T> BsonDocumentBuilder fieldOmitDefaultValue(Field.WithDefault<T> field, T value) {
    if (field.getDefaultValue().equals(value)) {
      return this;
    }
    return this.field(field, value);
  }

  public <T> BsonDocumentBuilder fieldOmitDefaultValue(
      Field.Optional<T> field, Optional<T> value, T defaultValue) {
    if (value.isEmpty() || defaultValue.equals(value.get())) {
      return this;
    }
    return this.field(field, value);
  }

  public <T> BsonDocumentBuilder join(BsonDocument document) {
    for (String fieldName : document.keySet()) {
      this.elements.add(new BsonElement(fieldName, document.get(fieldName)));
    }
    return this;
  }

  public BsonDocument build() {
    return new BsonDocument(this.elements);
  }
}
