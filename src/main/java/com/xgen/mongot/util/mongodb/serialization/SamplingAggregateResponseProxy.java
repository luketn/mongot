package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.bson.BsonSerialization;
import java.util.List;
import org.bson.RawBsonDocument;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class SamplingAggregateResponseProxy {

  private static final String OK_FIELD = "ok";
  private static final String CURSOR_FIELD = "cursor";

  private final double ok;
  private final CursorProxy cursor;

  /** Constructs a new ChangeStreamAggregateResponseProxy. */
  @BsonCreator
  public SamplingAggregateResponseProxy(
      @BsonProperty(OK_FIELD) Double ok, @BsonProperty(CURSOR_FIELD) CursorProxy cursor) {
    BsonSerialization.throwIfMissingProperty(ok, "ok");
    BsonSerialization.throwIfMissingProperty(cursor, "cursor");
    this.ok = ok;
    this.cursor = cursor;
  }

  public static class CursorProxy {

    private static final String FIRST_BATCH_FIELD = "firstBatch";
    private final List<RawBsonDocument> firstBatch;

    /** Constructs a new CursorProxy. */
    @BsonCreator
    public CursorProxy(@BsonProperty(FIRST_BATCH_FIELD) List<RawBsonDocument> firstBatch) {
      BsonSerialization.throwIfMissingProperty(firstBatch, "firstBatch");
      this.firstBatch = firstBatch;
    }

    public List<RawBsonDocument> getFirstBatch() {
      return this.firstBatch;
    }
  }

  public double getOk() {
    return this.ok;
  }

  public CursorProxy getCursor() {
    return this.cursor;
  }
}
