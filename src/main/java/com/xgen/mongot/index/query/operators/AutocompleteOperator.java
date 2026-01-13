package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record AutocompleteOperator(
    Score score,
    FieldPath path,
    List<String> query,
    Optional<FuzzyOption> fuzzy,
    TokenOrder tokenOrder)
    implements Operator {

  public static class Fields {
    public static final Field.Required<String> PATH =
        Field.builder("path").stringField().required();
    public static final Field.Required<List<String>> QUERY =
        Field.builder("query")
            .singleValueOrListOf(Value.builder().stringValue().mustNotBeEmpty().required())
            .mustNotBeEmpty()
            .required();
    public static final Field.Optional<FuzzyOption> FUZZY =
        Field.builder("fuzzy")
            .classField(FuzzyOption::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    public static final Field.WithDefault<TokenOrder> TOKEN_ORDER =
        Field.builder("tokenOrder")
            .enumField(TokenOrder.class)
            .asCamelCase()
            .optional()
            .withDefault(TokenOrder.ANY);
  }

  public enum TokenOrder {
    ANY,
    SEQUENTIAL
  }

  public static AutocompleteOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new AutocompleteOperator(
        Operators.parseScore(parser),
        FieldPath.parse(parser.getField(Fields.PATH).unwrap()),
        Operators.parseQuery(parser),
        parser.getField(Fields.FUZZY).unwrap(),
        parser.getField(Fields.TOKEN_ORDER).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilder(score())
        .field(Fields.PATH, this.path.toString())
        .field(Fields.QUERY, this.query)
        .field(Fields.FUZZY, this.fuzzy)
        .field(Fields.TOKEN_ORDER, this.tokenOrder)
        .build();
  }

  @Override
  public Type getType() {
    return Type.AUTOCOMPLETE;
  }
}
