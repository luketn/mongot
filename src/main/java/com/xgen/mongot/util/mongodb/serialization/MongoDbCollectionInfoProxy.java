package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

public class MongoDbCollectionInfoProxy {

  private static final String TYPE_KEY = "type";
  private static final String COLLECTION_TYPE = "collection";
  private static final String NAME_KEY = "name";
  private static final String INFO_KEY = "info";
  private static final String UUID_INFO_KEY = "uuid";

  private final String name;

  private final UUID uuid;

  public MongoDbCollectionInfoProxy(String name, UUID uuid) {
    Check.argNotNull(name, "name");
    Check.argNotNull(uuid, "uuid");

    this.name = name;
    this.uuid = uuid;
  }

  /**
   * Attempts to create a MongoDbCollectionInfoProxy from the supplied BsonDocument. Returns empty
   * if the BsonDocument does not match the expected schema.
   */
  public static Optional<MongoDbCollectionInfoProxy> fromBsonDocument(BsonDocument document) {
    Check.argNotNull(document, "document");

    BsonValue typeValue = document.get(TYPE_KEY);
    if (typeValue == null
        || !typeValue.isString()
        || !typeValue.asString().getValue().equals(COLLECTION_TYPE)) {
      return Optional.empty();
    }

    BsonValue nameValue = document.get(NAME_KEY);
    if (nameValue == null || !nameValue.isString()) {
      return Optional.empty();
    }
    String name = nameValue.asString().getValue();

    BsonValue infoValue = document.get(INFO_KEY);
    if (infoValue == null || !infoValue.isDocument()) {
      return Optional.empty();
    }
    BsonDocument info = infoValue.asDocument();

    BsonValue uuidValue = info.get(UUID_INFO_KEY);
    if (uuidValue == null || !uuidValue.isBinary()) {
      return Optional.empty();
    }
    BsonBinary uuidBinary = uuidValue.asBinary();
    if (!BsonBinarySubType.isUuid(uuidBinary.getType())) {
      return Optional.empty();
    }
    UUID uuid = uuidBinary.asUuid();

    return Optional.of(new MongoDbCollectionInfoProxy(name, uuid));
  }

  public String getName() {
    return this.name;
  }

  public UUID getUuid() {
    return this.uuid;
  }

  /** Returns the proxy as a BsonDocument. */
  public BsonDocument toBson() {
    BsonDocument infoDoc = new BsonDocument().append(UUID_INFO_KEY, new BsonBinary(this.uuid));
    return new BsonDocument()
        .append(TYPE_KEY, new BsonString(COLLECTION_TYPE))
        .append(NAME_KEY, new BsonString(this.name))
        .append(INFO_KEY, infoDoc);
  }
}
