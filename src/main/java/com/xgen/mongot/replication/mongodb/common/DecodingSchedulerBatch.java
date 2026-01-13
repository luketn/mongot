package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.SchedulerBatch;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;

public class DecodingSchedulerBatch extends SchedulerBatch {
  public final List<RawBsonDocument> events;
  public final DocumentBatchDecoder decoder;
  public final GenerationId generationId;
  public final IndexMetricsUpdater.ReplicationMetricsUpdater replicationMetricsUpdater;

  DecodingSchedulerBatch(
      List<RawBsonDocument> events,
      Priority priority,
      DocumentBatchDecoder decoder,
      CompletableFuture<Void> future,
      GenerationId generationId,
      Optional<ObjectId> attemptId,
      IndexMetricsUpdater.ReplicationMetricsUpdater replicationMetricsUpdater) {
    super(priority, future, generationId, attemptId);
    this.events = events;
    this.decoder = decoder;
    this.generationId = generationId;
    this.replicationMetricsUpdater = replicationMetricsUpdater;
  }

  @Override
  public String toString() {
    return "DecodingSchedulerBatch{"
        + "size="
        + this.events.size()
        + ", priority="
        + this.priority
        + ", generationId="
        + this.generationId
        + ", sequenceNumber="
        + this.sequenceNumber
        + '}';
  }

  @Override
  public int size() {
    return this.events.size();
  }
}
