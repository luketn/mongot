package com.xgen.mongot.replication.mongodb.common;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.xgen.mongot.util.mongodb.SamplingAggregateCommand;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.SamplingAggregateResponseProxy;
import java.util.List;
import java.util.Optional;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

public class CollectionSamplingMongoClient {

  private final MongoClient mongoClient;

  public CollectionSamplingMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  /**
   * Returns collection sampling results, limited in size by the provided number. In case when
   * documents are too large, the result might contain fewer results than the specified limit.
   *
   * <p>Note that in future the implementation can be simplified. Once we don't need to support
   * MongoDB 4.2, a new $bsonSize operator can be used to calculate avgSize on the DB side.
   */
  public List<RawBsonDocument> getSamples(
      MongoNamespace namespace,
      int sampleLimit,
      Optional<ImmutableList<Bson>> viewDefinedStages,
      Optional<Bson> projection) {

    var command =
        new SamplingAggregateCommand.Builder()
            .collection(namespace.getCollectionName())
            .batchSize(sampleLimit)
            .sampleLimit(sampleLimit);

    viewDefinedStages.ifPresent(command::viewDefinedStages);

    projection.ifPresent(
        projectionFields ->
            command.projectStage(Aggregates.project(projectionFields).toBsonDocument()));

    var proxy =
        this.mongoClient
            .getDatabase(namespace.getDatabaseName())
            .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
            .withReadConcern(ReadConcern.LOCAL)
            .runCommand(command.build().toProxy(), SamplingAggregateResponseProxy.class);

    return proxy.getCursor().getFirstBatch();
  }
}
