package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.util.mongodb.ChangeStreamResponse;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

/**
 * ChangeStreamBatch contains the minimal amount of data that the {@code ChangeStreamBufferManager}
 * and {@code ChangeStreamIndexManager} need for a given batch of change stream events.
 */
public class ChangeStreamBatch {

  private final List<RawBsonDocument> events;
  private final BsonDocument postBatchResumeToken;
  private final BsonTimestamp commandOperationTime;

  public ChangeStreamBatch(
      List<RawBsonDocument> events,
      BsonDocument postBatchResumeToken,
      BsonTimestamp commandOperationTime) {
    this.events = events;
    this.postBatchResumeToken = postBatchResumeToken;
    this.commandOperationTime = commandOperationTime;
  }

  public static ChangeStreamBatch fromResponse(ChangeStreamResponse response) {
    return new ChangeStreamBatch(
        response.getBatch(), response.getPostBatchResumeToken(), response.getOperationTime());
  }

  public List<RawBsonDocument> getRawEvents() {
    return this.events;
  }

  public BsonDocument getPostBatchResumeToken() {
    return this.postBatchResumeToken;
  }

  /**
   * Gets the operation time at which the command that returned this batch ran.
   *
   * <p>This is not directly related to the operation time of the contained change stream documents.
   */
  public BsonTimestamp getCommandOperationTime() {
    return this.commandOperationTime;
  }
}
