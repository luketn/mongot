package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.util.mongodb.serialization.ChangeStreamAggregateResponseProxy;
import com.xgen.mongot.util.mongodb.serialization.ChangeStreamGetMoreResponseProxy;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

/**
 * ChangeStreamResponse represents the response from either the initial aggregate command or a
 * subsequent getMore on a change stream.
 */
public class ChangeStreamResponse {

  private final long id;
  private final List<RawBsonDocument> batch;
  private final BsonDocument postBatchResumeToken;
  private final BsonTimestamp operationTime;

  ChangeStreamResponse(
      long id,
      List<RawBsonDocument> batch,
      BsonDocument postBatchResumeToken,
      BsonTimestamp operationTime) {
    this.id = id;
    this.batch = batch;
    this.postBatchResumeToken = postBatchResumeToken;
    this.operationTime = operationTime;
  }

  /** Constructs a new ChangeStreamResponse from the given ChangeStreamAggregateResponseProxy. */
  public static ChangeStreamResponse fromProxy(ChangeStreamAggregateResponseProxy proxy) {
    checkState(
        proxy.getOk() == 1.0,
        "ChangeStreamAggregateResponseProxy had unexpected ok field value: %s",
        proxy.getOk());

    ChangeStreamAggregateResponseProxy.CursorProxy cursor = proxy.getCursor();
    return new ChangeStreamResponse(
        cursor.getId(),
        cursor.getFirstBatch(),
        cursor.getPostBatchResumeToken(),
        proxy.getOperationTime());
  }

  /** Constructs a new ChangeStreamResponse from the given ChangeStreamGetMoreResponseProxy. */
  public static ChangeStreamResponse fromProxy(ChangeStreamGetMoreResponseProxy proxy) {
    checkState(
        proxy.getOk() == 1.0,
        "ChangeStreamGetMoreResponseProxy had unexpected ok field value: %s",
        proxy.getOk());

    ChangeStreamGetMoreResponseProxy.CursorProxy cursor = proxy.getCursor();
    return new ChangeStreamResponse(
        cursor.getId(),
        cursor.getNextBatch(),
        cursor.getPostBatchResumeToken(),
        proxy.getOperationTime());
  }

  public long getId() {
    return this.id;
  }

  public List<RawBsonDocument> getBatch() {
    return this.batch;
  }

  public BsonDocument getPostBatchResumeToken() {
    return this.postBatchResumeToken;
  }

  public BsonTimestamp getOperationTime() {
    return this.operationTime;
  }
}
