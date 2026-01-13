package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import java.util.Objects;
import org.bson.BsonDocument;

public class FlattenGraphTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.AnyToStream {

  @Override
  public Type getType() {
    return Type.FLATTEN_GRAPH;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof FlattenGraphTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
