package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.util.mongodb.serialization.GetMoreResponseProxy;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

/** GetMoreResponse represents the response from a getMore on a cursor that has been opened. */
public class GetMoreResponse {

  private final long id;
  private final List<RawBsonDocument> batch;

  private final Optional<BsonDocument> postBatchResumeToken;

  GetMoreResponse(
      long id, List<RawBsonDocument> batch, Optional<BsonDocument> postBatchResumeToken) {
    this.id = id;
    this.batch = batch;
    this.postBatchResumeToken = postBatchResumeToken;
  }

  /** Constructs a new GetMoreResponse from the given GetMoreResponseProxy. */
  public static GetMoreResponse fromProxy(GetMoreResponseProxy proxy) {
    checkState(
        proxy.getOk() == 1.0,
        "GetMoreResponseProxy had unexpected ok field value: %s",
        proxy.getOk());

    GetMoreResponseProxy.CursorProxy cursor = proxy.getCursor();
    return new GetMoreResponse(cursor.getId(), cursor.getNextBatch(), cursor.getPostResumeToken());
  }

  public long getId() {
    return this.id;
  }

  public List<RawBsonDocument> getBatch() {
    return this.batch;
  }

  public Optional<BsonDocument> getPostBatchResumeToken() {
    return this.postBatchResumeToken;
  }
}
