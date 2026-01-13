package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;

public interface ExpectedResult extends DocumentEncodable {

  class Fields {
    static final Field.WithDefault<ExpectedResult.Type> TYPE =
        Field.builder("type")
            .enumField(ExpectedResult.Type.class)
            .asCamelCase()
            .optional()
            .withDefault(Type.SINGLE_ITEM);
  }

  Type getType();

  List<ExpectedResultItem> getResults();

  static ExpectedResult fromBson(DocumentParser parser) throws BsonParseException {
    Type type = parser.getField(Fields.TYPE).unwrap();
    return switch (type) {
      case SINGLE_ITEM -> ExpectedResultItem.fromBson(parser);
      case GROUP -> ExpectedResultGroup.fromBson(parser);
    };
  }

  default ExpectedResultItem asItem() {
    throwIfNotType(Type.SINGLE_ITEM);
    return (ExpectedResultItem) this;
  }

  default ExpectedResultGroup asGroup() {
    throwIfNotType(Type.GROUP);
    return (ExpectedResultGroup) this;
  }

  private void throwIfNotType(Type expectedType) {
    Check.expectedType(expectedType, getType());
  }

  enum Type {
    SINGLE_ITEM,
    GROUP,
  }
}
