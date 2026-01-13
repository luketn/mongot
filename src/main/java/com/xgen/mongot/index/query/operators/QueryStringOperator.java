package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonValue;

/**
 * Definition for queryString: operator.
 *
 * @param score score
 * @param query query
 * @param defaultPath defaultPath
 */
public record QueryStringOperator(Score score, String query, String defaultPath)
    implements Operator {

  public static class Fields {
    public static final Field.Required<String> DEFAULT_PATH =
        Field.builder("defaultPath").stringField().required();

    public static final Field.Required<String> QUERY =
        Field.builder("query").stringField().mustNotBeEmpty().required();
  }

  /** Deserializes a QueryStringOperator from the supplied DocumentParser. */
  public static QueryStringOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new QueryStringOperator(
        Operators.parseScore(parser),
        parser.getField(Fields.QUERY).unwrap(),
        parser.getField(Fields.DEFAULT_PATH).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.DEFAULT_PATH, this.defaultPath)
        .field(Fields.QUERY, this.query)
        .build();
  }

  @Override
  public Type getType() {
    return Type.QUERY_STRING;
  }
}
