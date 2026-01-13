package com.xgen.mongot.index.analyzer.custom;

import java.util.Objects;
import org.bson.BsonDocument;

public class IcuNormalizeCharFilterDefinition extends CharFilterDefinition {
  @Override
  public Type getType() {
    return Type.ICU_NORMALIZE;
  }

  @Override
  BsonDocument charFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof IcuNormalizeCharFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
