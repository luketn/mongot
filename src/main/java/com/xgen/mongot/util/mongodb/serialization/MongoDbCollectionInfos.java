package com.xgen.mongot.util.mongodb.serialization;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoNamespace;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class MongoDbCollectionInfos {

  public final ImmutableMap<MongoNamespace, MongoDbCollectionInfo> infos;

  public MongoDbCollectionInfos(ImmutableMap<MongoNamespace, MongoDbCollectionInfo> infos) {
    this.infos = infos;
  }

  public Optional<MongoDbCollectionInfo> getCollectionInfo(MongoNamespace namespace) {
    return Optional.ofNullable(this.infos.get(namespace));
  }

  public Optional<MongoDbCollectionInfo> getCollectionInfo(String database, String collection) {
    return Optional.ofNullable(this.infos.get(new MongoNamespace(database, collection)));
  }

  public ImmutableMap<MongoNamespace, MongoDbCollectionInfo> getAll() {
    return this.infos;
  }

  public Set<UUID> getAllCollectionUuids() {
    return this.infos.values().stream()
        .filter(MongoDbCollectionInfo.Collection.class::isInstance)
        .map(MongoDbCollectionInfo.Collection.class::cast)
        .map(info -> info.info().uuid())
        .collect(Collectors.toSet());
  }
}
