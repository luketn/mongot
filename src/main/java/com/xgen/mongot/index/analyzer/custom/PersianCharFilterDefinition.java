package com.xgen.mongot.index.analyzer.custom;

import java.util.Objects;
import org.bson.BsonDocument;

public class PersianCharFilterDefinition extends CharFilterDefinition {
  @Override
  public Type getType() {
    return Type.PERSIAN;
  }

  @Override
  BsonDocument charFilterToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof PersianCharFilterDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
