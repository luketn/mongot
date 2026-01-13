package com.xgen.testing.mongot.cursor.serialization;

import com.xgen.mongot.cursor.serialization.MongotCursorResult;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;
import org.bson.BsonArray;
import org.bson.BsonValue;

public class MongotCursorResultBuilder {
  private Optional<Long> id = Optional.empty();
  private Optional<List<BsonValue>> nextBatch = Optional.empty();
  private Optional<String> namespace = Optional.empty();
  private Optional<MongotCursorResult.Type> type = Optional.empty();

  public static MongotCursorResultBuilder builder() {
    return new MongotCursorResultBuilder();
  }

  public MongotCursorResultBuilder id(Long id) {
    this.id = Optional.of(id);
    return this;
  }

  public MongotCursorResultBuilder nextBatch(List<BsonValue> nextBatch) {
    this.nextBatch = Optional.of(nextBatch);
    return this;
  }

  public MongotCursorResultBuilder namespace(String namespace) {
    this.namespace = Optional.of(namespace);
    return this;
  }

  public MongotCursorResultBuilder type(MongotCursorResult.Type type) {
    this.type = Optional.of(type);
    return this;
  }

  public MongotCursorResult build() {
    Check.isPresent(this.id, "id");
    Check.isPresent(this.nextBatch, "nextBatch");
    Check.isPresent(this.namespace, "namespace");

    return new MongotCursorResult(
        this.id.get(), new BsonArray(this.nextBatch.get()), this.namespace.get(), this.type);
  }
}
