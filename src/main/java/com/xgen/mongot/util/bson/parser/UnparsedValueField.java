package com.xgen.mongot.util.bson.parser;

import org.bson.BsonType;
import org.bson.BsonValue;

/** Contains the Field/ValueBuilder for passthrough BsonValue parsing. */
public class UnparsedValueField {

  private static final ValueEncoder<BsonValue> ENCODER = x -> x;
  private static final ValueParser<BsonValue> PARSER =
      (context, val) -> {
        if (val.isNull()) {
          context.handleUnexpectedType("non-null", BsonType.NULL);
        }
        return val;
      };

  public static class FieldBuilder extends Field.TypedBuilder<BsonValue, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER);
    }

    private FieldBuilder(
        String name, ValueEncoder<BsonValue> encoder, ValueParser<BsonValue> parser) {
      super(name, encoder, parser);
    }

    @Override
    BuilderFactory<BsonValue, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<BsonValue, ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<BsonValue> encoder, ValueParser<BsonValue> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<BsonValue, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }
}
