package com.xgen.mongot.util.mongodb.serialization;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class CollectionStatsCommandProxy implements Bson {

  private static final String COLL_STATS_FIELD = "collStats";

  private final String collection;

  public CollectionStatsCommandProxy(String collection) {
    this.collection = collection;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    return new BsonDocument(COLL_STATS_FIELD, new BsonString(this.collection));
  }
}
