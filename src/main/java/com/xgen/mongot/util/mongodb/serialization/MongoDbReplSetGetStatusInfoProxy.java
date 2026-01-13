package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class MongoDbReplSetGetStatusInfoProxy {

  private static final String OPTIMES_KEY = "optimes";

  private final MongoDbReplSetGetStatusInfoOpTimesProxy optimes;

  public MongoDbReplSetGetStatusInfoProxy(MongoDbReplSetGetStatusInfoOpTimesProxy optimes) {
    Check.argNotNull(optimes, "optimes");

    this.optimes = optimes;
  }

  /**
   * Returns a MongoDbReplSetGetStatusInfoProxy created from the BsonDocument if it follows the
   * proper schema.
   */
  public static MongoDbReplSetGetStatusInfoProxy fromBsonDocument(BsonDocument document)
      throws MongoDbInvalidReplStatusFormatException {
    Check.argNotNull(document, "document");

    BsonValue optimesValue = document.get(OPTIMES_KEY);
    if (optimesValue == null) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("document did not include %s field", OPTIMES_KEY));
    }

    if (!optimesValue.isDocument()) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("%s field was not a document", OPTIMES_KEY));
    }

    MongoDbReplSetGetStatusInfoOpTimesProxy optimes =
        MongoDbReplSetGetStatusInfoOpTimesProxy.fromBsonDocument(optimesValue.asDocument());

    return new MongoDbReplSetGetStatusInfoProxy(optimes);
  }

  public MongoDbReplSetGetStatusInfoOpTimesProxy getOptimes() {
    return this.optimes;
  }
}
