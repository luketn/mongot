package com.xgen.mongot.util.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbDatabase.class);

  public static MongoDbCollectionInfos getCollectionInfos(
      MongoClient mongoClient, String databaseName) throws CheckedMongoException {
    return CheckedMongoException.checkMongoException(
        () -> {
          Map<MongoNamespace, MongoDbCollectionInfo> infos = new HashMap<>();

          ListCollectionsIterable<BsonDocument> collectionInfos =
              mongoClient.getDatabase(databaseName).listCollections(BsonDocument.class);

          for (BsonDocument collectionInfo : collectionInfos) {
            MongoDbCollectionInfo.fromBsonDocument(collectionInfo)
                .ifPresent(info -> infos.put(new MongoNamespace(databaseName, info.name()), info));
          }

          return new MongoDbCollectionInfos(ImmutableMap.copyOf(infos));
        });
  }

  public static MongoDbServerInfo getServerInfo(MongoClient mongoClient) {
    var currentVersion = getCurrentVersion(mongoClient);
    var rsId = getRsId(mongoClient);
    return new MongoDbServerInfo(currentVersion, rsId);
  }

  public static Optional<BsonDocument> getBuildInfo(MongoClient client) {
    return getOrLogException(
        "failed to retrieve buildInfo from server",
        () ->
            CheckedMongoException.checkMongoException(
                () ->
                    client
                        .getDatabase("admin")
                        .runCommand(new Document("buildInfo", true))
                        .toBsonDocument()));
  }

  public static Optional<MongoDbVersion> getCurrentVersion(MongoClient client) {
    return getOrLogException(
        "failed to retrieve mongoDbVersion from server",
        () ->
            MongoDbVersion.fromVersionArray(
                MongoDbDatabase.getBuildInfo(client)
                    .map(
                        buildInfo ->
                            buildInfo.getArray("versionArray").asArray().stream()
                                .map(BsonValue::asInt32)
                                .map(BsonInt32::getValue)
                                .toList())
                    .orElse(Collections.emptyList())));
  }

  public static Optional<String> getRsId(MongoClient client) {
    return getOrLogException(
        "failed to retrieve rsId from server",
        () ->
            CheckedMongoException.checkMongoException(
                () ->
                    client
                        .getDatabase("admin")
                        .runCommand(new Document("replSetGetConfig", 1))
                        .getEmbedded(List.of("config", "_id"), String.class)));
  }

  private static <T> Optional<T> getOrLogException(
      String logMessage, CheckedSupplier<T, CheckedMongoException> supplier) {
    try {
      return Optional.ofNullable(supplier.get());
    } catch (CheckedMongoException e) {
      LOG.atWarn().setCause(e).log(logMessage);
      return Optional.empty();
    }
  }

  public static MongoDbCollectionInfo getCollectionInfo(
      MongoClient mongoClient, String databaseName, String collectionName)
      throws CheckedMongoException {
    return getCollectionInfos(mongoClient, databaseName)
        .getCollectionInfo(databaseName, collectionName)
        .orElseThrow();
  }
}
