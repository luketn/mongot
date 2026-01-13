package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * MongoDbReplSetGetStatusInfoOpTimesProxy proxies the expected data in optimes returned by
 * rs.status().
 *
 * <p>For a description of the meaning of the different optimes, see:
 * https://docs.mongodb.com/manual/reference/command/replSetGetStatus/#replSetGetStatus.optimes
 */
public class MongoDbReplSetGetStatusInfoOpTimesProxy {

  private static final String LAST_COMMITTED_OP_TIME_KEY = "lastCommittedOpTime";
  private static final String READ_CONCERN_MAJORITY_OP_TIME_KEY = "readConcernMajorityOpTime";
  private static final String APPLIED_OP_TIME_KEY = "appliedOpTime";
  private static final String DURABLE_OP_TIME_KEY = "durableOpTime";

  private final MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy lastCommittedOpTime;
  private final MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy readConcernMajorityOpTime;
  private final MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy appliedOpTime;
  private final MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy durableOpTime;

  public MongoDbReplSetGetStatusInfoOpTimesProxy(
      MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy lastCommittedOptime,
      MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy readConcernMajorityOpTime,
      MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy appliedOpTime,
      MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy durableOpTime) {
    Check.argNotNull(lastCommittedOptime, "lastCommittedOpTime");
    Check.argNotNull(readConcernMajorityOpTime, "readConcernMajorityOpTime");
    Check.argNotNull(appliedOpTime, "appliedOpTime");
    Check.argNotNull(durableOpTime, "durableOpTime");

    this.lastCommittedOpTime = lastCommittedOptime;
    this.readConcernMajorityOpTime = readConcernMajorityOpTime;
    this.appliedOpTime = appliedOpTime;
    this.durableOpTime = durableOpTime;
  }

  /**
   * Returns a MongoDbReplSetGetStatusInfoOpTimesProxy created from the BsonDocument if it follows
   * the proper schema.
   */
  public static MongoDbReplSetGetStatusInfoOpTimesProxy fromBsonDocument(BsonDocument document)
      throws MongoDbInvalidReplStatusFormatException {
    Check.argNotNull(document, "document");

    MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy lastCommitted =
        optimeFromKey(document, LAST_COMMITTED_OP_TIME_KEY);

    MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy readConcernMajority =
        optimeFromKey(document, READ_CONCERN_MAJORITY_OP_TIME_KEY);

    MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy applied =
        optimeFromKey(document, APPLIED_OP_TIME_KEY);

    MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy durable =
        optimeFromKey(document, DURABLE_OP_TIME_KEY);

    return new MongoDbReplSetGetStatusInfoOpTimesProxy(
        lastCommitted, readConcernMajority, applied, durable);
  }

  public MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy getLastCommittedOpTime() {
    return this.lastCommittedOpTime;
  }

  public MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy getReadConcernMajorityOpTime() {
    return this.readConcernMajorityOpTime;
  }

  public MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy getAppliedOpTime() {
    return this.appliedOpTime;
  }

  public MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy getDurableOpTime() {
    return this.durableOpTime;
  }

  private static MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy optimeFromKey(
      BsonDocument document, String key) throws MongoDbInvalidReplStatusFormatException {
    BsonValue value = document.get(key);
    if (value == null) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("optimes did not include field %s", key));
    }

    if (!value.isDocument()) {
      throw new MongoDbInvalidReplStatusFormatException(
          String.format("%s field was not a document", key));
    }

    return MongoDbReplSetGetStatusInfoOpTimesOpTimeProxy.fromBsonDocument(value.asDocument());
  }
}
