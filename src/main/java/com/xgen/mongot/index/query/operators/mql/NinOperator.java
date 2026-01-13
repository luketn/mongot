package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.operators.value.NonNullValue;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public record NinOperator(List<NonNullValue> values) implements ComparisonOperator {

  private static class Values {
    private static final com.xgen.mongot.util.bson.parser.Value.Required<List<NonNullValue>> VALUE =
        com.xgen.mongot.util.bson.parser.Value.builder()
            .listOf(
                com.xgen.mongot.util.bson.parser.Value.builder()
                    .classValue(NonNullValue::fromBson)
                    .required())
            .mustNotBeEmpty()
            .validate(
                input ->
                    Value.containsDistinctType(input)
                        ? Optional.of("must have elements of the same type")
                        : Optional.empty())
            .required();
  }

  @Override
  public ValueType getValueType() {
    // array cannot be empty, values are of the same type
    return this.values.getFirst().getType();
  }

  @Override
  public Category getCategory() {
    return Category.NIN;
  }

  public NonNullValueType getNonNullValueType() {
    // array cannot be empty, values are of the same type
    return this.values.getFirst().getNonNullType();
  }

  @Override
  public BsonValue operatorToBson() {
    return new BsonArray(this.values.stream().map(Value::toBson).collect(Collectors.toList()));
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.NIN, Optional.of(this)).build();
  }

  static NinOperator fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    var values = Values.VALUE.getParser().parse(context, bsonValue);
    return new NinOperator(values);
  }
}
