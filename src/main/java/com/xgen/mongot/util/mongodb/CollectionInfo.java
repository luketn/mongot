package com.xgen.mongot.util.mongodb;

import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

public class CollectionInfo {

  private static final String INFO_UUID_FIELD = "info.uuid";

  /**
   * Returns a filter that can be used on listCollections() to filter for the collection with the
   * supplied UUID.
   */
  public static Bson uuidFilter(UUID collectionUuid) {
    return new BsonDocument(INFO_UUID_FIELD, new BsonBinary(collectionUuid));
  }
}
