package com.xgen.mongot.index.query.scores;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record ValueBoostScore(float boost) implements Score {

  static class Fields {
    static final Field.Optional<Float> VALUE =
        Field.builder("value").floatField().mustBeFinite().mustBePositive().optional().noDefault();
  }

  static final ValueBoostScore DEFAULT_BOOST_DEFINITION = new ValueBoostScore(1f);

  @Override
  public BsonValue scoreToBson() {
    return BsonDocumentBuilder.builder().field(Fields.VALUE, Optional.of(this.boost)).build();
  }

  @Override
  public Type getType() {
    return Type.VALUE_BOOST;
  }

  @Override
  public List<FieldPath> getChildPaths() {
    return List.of();
  }

  public boolean isDefault() {
    return Float.compare(this.boost, DEFAULT_BOOST_DEFINITION.boost) == 0;
  }
}
