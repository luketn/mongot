package com.xgen.mongot.util.bson.parser;

import org.bson.BsonDateTime;

public class BsonDateTimeField {

  private static final ValueEncoder<BsonDateTime> ENCODER = x -> x;
  private static final ValueParser<BsonDateTime> PARSER =
      (context, val) -> {
        if (!val.isDateTime()) {
          context.handleUnexpectedType(TypeDescription.DATE_TIME, val.getBsonType());
        }
        return val.asDateTime();
      };

  public static class FieldBuilder extends Field.TypedBuilder<BsonDateTime, FieldBuilder> {

    FieldBuilder(String name) {
      this(name, ENCODER, PARSER);
    }

    private FieldBuilder(
        String name, ValueEncoder<BsonDateTime> encoder, ValueParser<BsonDateTime> parser) {
      super(name, encoder, parser);
    }

    @Override
    BuilderFactory<BsonDateTime, FieldBuilder> getBuilderFactory() {
      return FieldBuilder::new;
    }
  }

  public static class ValueBuilder extends Value.TypedBuilder<BsonDateTime, ValueBuilder> {

    ValueBuilder() {
      this(ENCODER, PARSER);
    }

    private ValueBuilder(ValueEncoder<BsonDateTime> encoder, ValueParser<BsonDateTime> parser) {
      super(encoder, parser);
    }

    @Override
    BuilderFactory<BsonDateTime, ValueBuilder> getBuilderFactory() {
      return ValueBuilder::new;
    }
  }
}
