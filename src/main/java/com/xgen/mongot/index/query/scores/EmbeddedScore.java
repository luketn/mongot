package com.xgen.mongot.index.query.scores;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

public record EmbeddedScore(
    com.xgen.mongot.index.query.scores.EmbeddedScore.Aggregate aggregate, Score outerScore)
    implements Score {

  public enum Aggregate {
    SUM,
    MAXIMUM,
    MINIMUM,
    MEAN
  }

  public static class Fields {
    public static final Field.WithDefault<Aggregate> AGGREGATE =
        Field.builder("aggregate")
            .enumField(Aggregate.class)
            .asCamelCase()
            .optional()
            .withDefault(Aggregate.SUM);
    public static final Field.WithDefault<Score> OUTER_SCORE =
        Field.builder("outerScore")
            .classField(Score::fromBson)
            .disallowUnknownFields()
            .optional()
            .withDefault(Score.defaultScore());
  }

  @Override
  public Type getType() {
    return Type.EMBEDDED;
  }

  @Override
  public List<FieldPath> getChildPaths() {
    return this.outerScore.getChildPaths();
  }

  public static EmbeddedScore fromBson(DocumentParser parser) throws BsonParseException {
    return new EmbeddedScore(
        parser.getField(Fields.AGGREGATE).unwrap(), parser.getField(Fields.OUTER_SCORE).unwrap());
  }

  @Override
  public BsonValue scoreToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.AGGREGATE, this.aggregate)
        .field(Fields.OUTER_SCORE, this.outerScore)
        .build();
  }
}
