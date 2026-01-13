package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

/**
 * DateFieldDefinition defines how to index fields of type "date".
 *
 * <p>This class is empty; there are no configuration options for a date field definition.
 */
public record DateFieldDefinition() implements FieldTypeDefinition, DatetimeFieldDefinition {
  static DateFieldDefinition fromBson(DocumentParser parser) {
    return new DateFieldDefinition();
  }

  @Override
  public Type getType() {
    return Type.DATE;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }
}
