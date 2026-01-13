package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public record EqOperator(Value value) implements ComparisonOperator {

  @Override
  public Category getCategory() {
    return Category.EQ;
  }

  @Override
  public ValueType getValueType() {
    return this.value.getType();
  }

  @Override
  public BsonValue operatorToBson() {
    return this.value.toBson();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.EQ, Optional.of(this)).build();
  }

  static EqOperator fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    return new EqOperator(Value.fromBson(context, bsonValue));
  }
}
