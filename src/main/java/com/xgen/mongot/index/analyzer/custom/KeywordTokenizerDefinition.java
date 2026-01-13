package com.xgen.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import java.util.Objects;
import org.bson.BsonDocument;

public class KeywordTokenizerDefinition extends TokenizerDefinition
    implements TokenStreamTypeAware.Stream {
  @Override
  public Type getType() {
    return Type.KEYWORD;
  }

  @Override
  BsonDocument tokenizerToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof KeywordTokenizerDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
