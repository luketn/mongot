package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import java.util.Objects;
import org.bson.BsonDocument;

public class IcuFoldingTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {
  @Override
  public Type getType() {
    return Type.ICU_FOLDING;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof IcuFoldingTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
