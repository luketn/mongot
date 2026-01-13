package com.xgen.mongot.index.query.points;

import java.util.UUID;
import org.bson.BsonBinary;

public record UuidPoint(UUID value) implements Point, Comparable<UuidPoint> {

  @Override
  public Type getType() {
    return Type.UUID;
  }

  @Override
  public int compareTo(UuidPoint o) {
    return this.value.compareTo(o.value());
  }

  // NOTE: possible issue with type
  @Override
  public BsonBinary toBson() {
    return new BsonBinary(this.value);
  }
}
