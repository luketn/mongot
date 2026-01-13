package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

// {$ne: 1}
public record NeOperator(Value value) implements ComparisonOperator {

  @Override
  public Category getCategory() {
    return Category.NE;
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
    return BsonDocumentBuilder.builder().field(Fields.NE, Optional.of(this)).build();
  }

  static NeOperator fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    return new NeOperator(Value.fromBson(context, bsonValue));
  }
}
