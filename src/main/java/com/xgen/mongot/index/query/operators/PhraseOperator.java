package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record PhraseOperator(
    Score score,
    List<UnresolvedStringPath> paths,
    List<String> query,
    int slop,
    Optional<String> synonyms)
    implements Operator {

  public static class Fields {
    public static final Field.WithDefault<Integer> SLOP =
        Field.builder("slop").intField().mustBeNonNegative().optional().withDefault(0);

    public static final Field.Optional<String> SYNONYMS =
        Field.builder("synonyms").stringField().mustNotBeEmpty().optional().noDefault();
  }

  public static PhraseOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new PhraseOperator(
        Operators.parseScore(parser),
        Operators.parseUnresolvedStringPath(parser),
        Operators.parseQuery(parser),
        parser.getField(Fields.SLOP).unwrap(),
        parser.getField(Fields.SYNONYMS).unwrap());
  }

  @Override
  public BsonValue operatorToBson() {
    return Operators.documentBuilderWithUnresolvedStringPath(score(), this.paths, this.query)
        .field(Fields.SLOP, this.slop)
        .field(Fields.SYNONYMS, this.synonyms)
        .build();
  }

  @Override
  public Type getType() {
    return Type.PHRASE;
  }
}
