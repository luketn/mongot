package com.xgen.mongot.util.bson.parser;

import org.bson.BsonBinary;
import org.bson.types.Binary;

public class BinaryField {
  private static final ValueEncoder<Binary> ENCODER = binary -> new BsonBinary(binary.getData());
  private static final ValueParser<Binary> PARSER =
      (context, value) -> {
        if (!value.isBinary()) {
          context.handleUnexpectedType(TypeDescription.BINARY, value.getBsonType());
        }
        return new Binary(value.asBinary().getData());
      };

  public static class FieldBuilder extends Field.TypedBuilder<Binary, FieldBuilder> {
    FieldBuilder(String name) {
      this(name, ENCODER, PARSER);
    }

    FieldBuilder(String name, ValueEncoder<Binary> encoder, ValueParser<Binary> parser) {
      super(name, encoder, parser);
    }

    @Override
    BuilderFactory<Binary, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<Binary, ValueBuilder> {
    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<Binary> encoder, ValueParser<Binary> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<Binary, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }
}
