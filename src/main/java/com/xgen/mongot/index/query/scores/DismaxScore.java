package com.xgen.mongot.index.query.scores;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.apache.commons.lang3.Range;
import org.bson.BsonValue;

public record DismaxScore(float tieBreakerScore) implements Score {

  public static class Fields {
    public static final Field.WithDefault<Float> TIE_BREAKER_SCORE =
        Field.builder("tieBreakerScore")
            .floatField()
            .mustBeWithinBounds(Range.of(0f, 1f))
            .optional()
            .withDefault(1f);
  }

  public static DismaxScore fromBson(DocumentParser parser) throws BsonParseException {
    return new DismaxScore(parser.getField(Fields.TIE_BREAKER_SCORE).unwrap());
  }

  @Override
  public BsonValue scoreToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.TIE_BREAKER_SCORE, this.tieBreakerScore)
        .build();
  }

  @Override
  public Type getType() {
    return Type.DISMAX;
  }

  @Override
  public List<FieldPath> getChildPaths() {
    return List.of();
  }
}
