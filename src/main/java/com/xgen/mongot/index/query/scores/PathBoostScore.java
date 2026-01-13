package com.xgen.mongot.index.query.scores;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public record PathBoostScore(FieldPath path, double undefined) implements Score {

  static class Fields {
    static final Field.Optional<String> PATH =
        Field.builder("path").stringField().optional().noDefault();
    static final Field.WithDefault<Double> UNDEFINED =
        Field.builder("undefined").doubleField().mustBeFinite().optional().withDefault(0.0d);
  }

  @Override
  public Type getType() {
    return Type.PATH_BOOST;
  }

  @Override
  public List<FieldPath> getChildPaths() {
    return List.of(this.path);
  }

  @Override
  public BsonValue scoreToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, Optional.of(this.path.toString()))
        .field(Fields.UNDEFINED, this.undefined)
        .build();
  }
}
