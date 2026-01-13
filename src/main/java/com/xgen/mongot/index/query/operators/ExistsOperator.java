package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

public record ExistsOperator(Score score, String path) implements Operator {

  public static class Fields {
    public static final Field.Required<String> PATH =
        Field.builder("path").stringField().required();
  }

  public static ExistsOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new ExistsOperator(Operators.parseScore(parser), parser.getField(Fields.PATH).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score()).field(Fields.PATH, this.path).build();
  }

  @Override
  public Type getType() {
    return Type.EXISTS;
  }
}
