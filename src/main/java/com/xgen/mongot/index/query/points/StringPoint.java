package com.xgen.mongot.index.query.points;

import org.bson.BsonString;
import org.bson.BsonValue;

public record StringPoint(String value) implements Point, Comparable<StringPoint> {

  @Override
  public Type getType() {
    return Type.STRING;
  }

  @Override
  public int compareTo(StringPoint o) {
    if (o == null || o.value == null) {
      return 0;
    }
    return this.value.compareTo(o.value);
  }

  @Override
  public BsonValue toBson() {
    return new BsonString(this.value);
  }
}
