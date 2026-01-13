package com.xgen.mongot.replication.mongodb.synonyms;

import static com.xgen.mongot.util.Check.checkState;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.CollectionScanCommandMongoClient.FutureWrapper;
import com.xgen.mongot.replication.mongodb.common.CollectionScanCommandMongoClient.NamespaceChangeCheck;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.DefaultChangeStreamMongoClient;
import com.xgen.mongot.replication.mongodb.common.FindCommandCollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.NoShardFoundException;
import com.xgen.mongot.replication.mongodb.common.SessionRefresher;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.ListShardsCommandProxy;
import com.xgen.mongot.util.mongodb.serialization.ListShardsResponseProxy;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.bson.BsonBoolean;

/** Create mongo client and command for synonym syncs. */
class SynonymSyncMongoClient {

  private final MongoClient mongoClient;
  private final SessionRefresher sessionRefresher;
  private final MeterRegistry meterRegistry;
  private final boolean isSharded;

  SynonymSyncMongoClient(
      boolean isSharded,
      MongoClient mongoClient,
      SessionRefresher sessionRefresher,
      MeterRegistry meterRegistry) {
    this.isSharded = isSharded;
    this.mongoClient = mongoClient;
    this.sessionRefresher = sessionRefresher;
    this.meterRegistry = meterRegistry;
  }

  static CollectionScanFindCommand collectionScanFindCommand(SynonymMappingDefinition definition) {
    return new CollectionScanFindCommand.Builder(definition.source().collection())
        .noCursorTimeout(BsonBoolean.TRUE)
        .readConcern(ReadConcern.MAJORITY)
        .build();
  }

  static ChangeStreamAggregateCommand changeStreamAggregateCommand(
      SynonymMappingDefinition definition, SynonymMappingHighWaterMark highWaterMark) {
    checkState(
        highWaterMark.isPresent(),
        "must have operationTime or postBatchResumeToken to start change stream");

    ChangeStreamAggregateCommand.Builder builder =
        new ChangeStreamAggregateCommand.Builder()
            .batchSize(1)
            .collection(definition.source().collection());

    if (highWaterMark.getOperationTime().isPresent()) {
      builder.startAtOperationTime(highWaterMark.getOperationTime().get());
    } else {
      // resume token specified
      //noinspection OptionalGetWithoutIsPresent (assertion at start of method guarantees presence)
      builder.startAfter(highWaterMark.getResumeToken().get());
    }
    return builder.build();
  }

  CollectionScanMongoClient<SynonymSyncException> getFindCommandClient(
      CollectionScanFindCommand findCommand, MongoNamespace namespace) throws SynonymSyncException {
    FutureWrapper<SynonymSyncException> futureWrapper = SynonymSyncException::wrapIfThrows;
    NamespaceChangeCheck<SynonymSyncException> namespaceChangeCheck = (ns) -> {}; // no op
    var session = futureWrapper.getOrWrapThrowable(this.mongoClient::startSession);
    var refreshingSession = this.sessionRefresher.register(session);

    return new FindCommandCollectionScanMongoClient<>(
        findCommand,
        this.mongoClient,
        refreshingSession,
        new MetricsFactory("indexing.synonymSyncCollectionScan", this.meterRegistry),
        namespace,
        namespaceChangeCheck,
        futureWrapper,
        SynonymSyncException::createTransient,
        Optional.empty());
  }

  ChangeStreamMongoClient<SynonymSyncException> getChangeStreamClient(
      ChangeStreamAggregateCommand aggregateCommand, MongoNamespace namespace)
      throws SynonymSyncException {
    return DefaultChangeStreamMongoClient.createSynonymSync(
        aggregateCommand,
        this.mongoClient,
        this.sessionRefresher,
        namespace,
        new MetricsFactory("indexing.synonymSyncChangeStream", this.meterRegistry));
  }

  void mongosHealthCheck() throws SynonymSyncException {
    if (this.isSharded) {
      SynonymSyncException.wrapIfThrows(
          // Throws an error if sharding is not fully complete.
          // If mongos isn't up, the command will error and we requeue synonym sync.
          // If shard isn't added to mongos, then we throw a transient SynonymSyncError and requeue
          // synonym sync (CLOUDP-140721).
          () -> {
            ListShardsResponseProxy response =
                this.mongoClient
                    .getDatabase("admin")
                    .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
                    .runCommand(
                        ListShardsCommandProxy.listShardsCommand(), ListShardsResponseProxy.class);
            if (response.getShards().size() <= 0) {
              throw new NoShardFoundException("Shards not added to MongoDB yet");
            }
          });
    }
  }

  boolean isSharded() {
    return this.isSharded;
  }
}
