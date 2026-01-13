package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record TextOperator(
    Score score,
    List<UnresolvedStringPath> paths,
    List<String> query,
    Optional<FuzzyOption> fuzzy,
    Optional<String> synonyms,
    Optional<MatchCriteria> matchCriteria)
    implements Operator {

  public enum MatchCriteria {
    ANY,
    ALL
  }

  public static class Fields {
    public static final Field.Optional<FuzzyOption> FUZZY =
        Field.builder("fuzzy")
            .classField(FuzzyOption::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    public static final Field.Optional<MatchCriteria> MATCH_CRITERIA =
        Field.builder("matchCriteria")
            .enumField(MatchCriteria.class)
            .asCamelCase()
            .optional()
            .noDefault();
    public static final Field.Optional<String> SYNONYMS =
        Field.builder("synonyms").stringField().mustNotBeEmpty().optional().noDefault();
  }

  public static TextOperator fromBson(DocumentParser parser) throws BsonParseException {
    parser.getGroup().atMostOneOf(parser.getField(Fields.FUZZY), parser.getField(Fields.SYNONYMS));
    return new TextOperator(
        Operators.parseScore(parser),
        Operators.parseUnresolvedStringPath(parser),
        Operators.parseQuery(parser),
        parser.getField(Fields.FUZZY).unwrap(),
        parser.getField(Fields.SYNONYMS).unwrap(),
        parser.getField(Fields.MATCH_CRITERIA).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilderWithUnresolvedStringPath(score(), this.paths, this.query)
        .field(Fields.FUZZY, this.fuzzy)
        .field(Fields.SYNONYMS, this.synonyms)
        .field(Fields.MATCH_CRITERIA, this.matchCriteria)
        .build();
  }

  @Override
  public Type getType() {
    return Type.TEXT;
  }
}
