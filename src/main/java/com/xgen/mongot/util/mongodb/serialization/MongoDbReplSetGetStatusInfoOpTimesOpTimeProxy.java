package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

public class MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy {

  private static final String TIMESTAMP_KEY = "ts";
  private static final String TERM_KEY = "t";

  private final BsonTimestamp timestamp;
  private final BsonInt64 term;

  public MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy(BsonTimestamp timestamp, BsonInt64 term) {
    Check.argNotNull(timestamp, "timestamp");
    Check.argNotNull(term, "term");

    this.timestamp = timestamp;
    this.term = term;
  }

  /**
   * Returns a MongoDbReplSetGetStatusInfoOpTimesProxy created from the BsonDocument if it follows
   * the proper schema.
   */
  public static MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy fromBsonDocument(
      BsonDocument document) throws MongoDbInvalidReplStatusFormatException {
    Check.argNotNull(document, "document");

    BsonValue timestampValue = document.get(TIMESTAMP_KEY);
    if (timestampValue == null) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("optime did not include field %s", TIMESTAMP_KEY));
    }
    if (!timestampValue.isTimestamp()) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("optime field %s was not a timestamp", TIMESTAMP_KEY));
    }

    BsonTimestamp timestamp = timestampValue.asTimestamp();

    BsonValue termValue = document.get(TERM_KEY);
    if (termValue == null) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("optime did not include field %s", TERM_KEY));
    }
    if (!termValue.isInt64()) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("optime field %s was not a timestamp", TERM_KEY));
    }

    BsonInt64 term = termValue.asInt64();

    return new MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy(timestamp, term);
  }

  public BsonTimestamp getTimestamp() {
    return this.timestamp;
  }

  public BsonInt64 getTerm() {
    return this.term;
  }
}
