package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public record ExistsOperator(boolean value) implements MqlFilterOperator {
  static class Fields {
    static final Field.Optional<ExistsOperator> EXISTS =
        Field.builder("$exists")
            .classField(ExistsOperator::fromBson, ExistsOperator::operatorToBson)
            .optional()
            .noDefault();
  }

  private static ExistsOperator fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    if (!bsonValue.isBoolean()) {
      return context.handleUnexpectedType("boolean", bsonValue.getBsonType());
    }

    return new ExistsOperator(bsonValue.asBoolean().getValue());
  }

  public BsonValue operatorToBson() {
    return new BsonBoolean(this.value);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.EXISTS, Optional.of(this)).build();
  }

  @Override
  public Category getCategory() {
    return Category.EXISTS;
  }
}
