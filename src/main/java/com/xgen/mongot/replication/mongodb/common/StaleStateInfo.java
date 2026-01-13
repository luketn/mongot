package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.status.StaleStatusReason;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Contains information about transition to the STALE status. */
public class StaleStateInfo implements DocumentEncodable {

  private static class Fields {

    private static final Field.Required<BsonTimestamp> LAST_OPTIME =
        Field.builder("lastOptime").bsonTimestampField().required();

    private static final Field.Required<StaleStatusReason> REASON =
        Field.builder("reason").enumField(StaleStatusReason.class).asUpperUnderscore().required();

    private static final Field.WithDefault<String> MESSAGE =
        Field.builder("message").stringField().optional().withDefault("");
  }

  /** last optime that have been seen before transitioning to STALE state */
  private final BsonTimestamp lastOptime;

  /** the reason of transitioning to STALE state */
  private final StaleStatusReason reason;

  private final String message;

  private StaleStateInfo(BsonTimestamp lastOptime, StaleStatusReason reason, String message) {
    this.lastOptime = lastOptime;
    this.reason = reason;
    this.message = message;
  }

  public static StaleStateInfo create(BsonTimestamp lastOptime, StaleStatusReason reason,
      String message) {
    return new StaleStateInfo(lastOptime, reason, message);
  }

  public static StaleStateInfo create(BsonTimestamp lastOptime, StaleStatusReason reason) {
    return new StaleStateInfo(lastOptime, reason, reason.formatMessage());
  }

  public static StaleStateInfo fromBson(DocumentParser parser) throws BsonParseException {
    return create(
        parser.getField(Fields.LAST_OPTIME).unwrap(),
        parser.getField(Fields.REASON).unwrap(),
        parser.getField(Fields.MESSAGE).unwrap());
  }

  public static StaleStateInfo fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.LAST_OPTIME, this.lastOptime)
        .field(Fields.REASON, this.reason)
        .field(Fields.MESSAGE, this.message)
        .build();
  }

  public String toJson() {
    return toBson().toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
  }

  public BsonTimestamp getLastOptime() {
    return this.lastOptime;
  }

  public StaleStatusReason getReason() {
    return this.reason;
  }

  public String getMessage() {
    return this.message.isEmpty() ? this.reason.formatMessage() : this.message;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof StaleStateInfo)) {
      return false;
    }
    StaleStateInfo that = (StaleStateInfo) object;
    return Objects.equals(this.lastOptime, that.lastOptime)
        && this.reason == that.reason
        && Objects.equals(this.message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.lastOptime, this.reason, this.message);
  }
}
