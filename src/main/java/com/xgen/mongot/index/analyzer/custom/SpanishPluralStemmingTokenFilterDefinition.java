package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import java.util.Objects;
import org.bson.BsonDocument;

public class SpanishPluralStemmingTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {

  @Override
  public Type getType() {
    return Type.SPANISH_PLURAL_STEMMING;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SpanishPluralStemmingTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
