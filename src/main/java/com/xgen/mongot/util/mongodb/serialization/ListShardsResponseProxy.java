package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.bson.BsonSerialization;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class ListShardsResponseProxy {

  private static final String SHARDS_FIELD = "shards";

  private final List<BsonDocument> shards;

  @BsonCreator
  public ListShardsResponseProxy(@BsonProperty(SHARDS_FIELD) List<BsonDocument> shards) {
    BsonSerialization.throwIfMissingProperty(shards, "shards");
    this.shards = shards;
  }

  public List<BsonDocument> getShards() {
    return this.shards;
  }
}
