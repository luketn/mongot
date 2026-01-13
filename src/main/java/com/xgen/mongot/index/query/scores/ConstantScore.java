package com.xgen.mongot.index.query.scores;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonValue;

public record ConstantScore(float score) implements Score {

  public static class Fields {
    public static final Field.Required<Float> VALUE =
        Field.builder("value").floatField().mustBeFinite().mustBeNonNegative().required();
  }

  public static ConstantScore fromBson(DocumentParser parser) throws BsonParseException {
    return new ConstantScore(parser.getField(Fields.VALUE).unwrap());
  }

  @Override
  public BsonValue scoreToBson() {
    return BsonDocumentBuilder.builder().field(Fields.VALUE, this.score).build();
  }

  @Override
  public Type getType() {
    return Type.CONSTANT;
  }

  @Override
  public List<FieldPath> getChildPaths() {
    return List.of();
  }
}
