package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.BsonUtils;
import org.bson.BsonDocument;

public class RemoveDuplicatesTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.OutputsSameTypeAsInput {

  @Override
  public Type getType() {
    return Type.REMOVE_DUPLICATES;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonUtils.emptyDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RemoveDuplicatesTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return RemoveDuplicatesTokenFilterDefinition.class.hashCode();
  }
}
