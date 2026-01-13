package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTransformationStage;
import com.xgen.mongot.util.BsonUtils;
import org.bson.BsonDocument;

public class KeywordRepeatTokenFilterDefinition extends TokenFilterDefinition
    implements TokenStreamTransformationStage.AnyToGraph {

  @Override
  public Type getType() {
    return Type.KEYWORD_REPEAT;
  }

  @Override
  BsonDocument tokenFilterToBson() {
    return BsonUtils.emptyDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof KeywordRepeatTokenFilterDefinition;
  }

  @Override
  public int hashCode() {
    return KeywordRepeatTokenFilterDefinition.class.hashCode();
  }
}
