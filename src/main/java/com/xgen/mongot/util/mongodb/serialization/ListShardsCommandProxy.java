package com.xgen.mongot.util.mongodb.serialization;

import org.bson.BsonDocument;
import org.bson.BsonInt32;

public class ListShardsCommandProxy {
  public static BsonDocument listShardsCommand() {
    return new BsonDocument("listShards", new BsonInt32(1));
  }
}
