package com.xgen.mongot.catalogservice;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Instant;
import java.util.Objects;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record ServerStateEntry(ObjectId serverId, String serverName, Instant lastHeartbeatTs)
    implements DocumentEncodable {

  public ServerStateEntry {
    Objects.requireNonNull(serverId);
    Objects.requireNonNull(serverName);
    Objects.requireNonNull(lastHeartbeatTs);
  }

  abstract static class Fields {
    static final Field.Required<ObjectId> ID = Field.builder("_id").objectIdField().required();

    static final Field.Required<String> SERVER_NAME =
        Field.builder("serverName").stringField().required();

    static final Field.Required<BsonDateTime> LAST_HEARTBEAT_TS =
        Field.builder("lastHeartbeatTs").bsonDateTimeField().required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.serverId)
        .field(Fields.SERVER_NAME, this.serverName)
        .field(Fields.LAST_HEARTBEAT_TS, new BsonDateTime(this.lastHeartbeatTs.toEpochMilli()))
        .build();
  }

  public static ServerStateEntry fromBson(DocumentParser parser) throws BsonParseException {
    return new ServerStateEntry(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.SERVER_NAME).unwrap(),
        Instant.ofEpochMilli(parser.getField(Fields.LAST_HEARTBEAT_TS).unwrap().getValue()));
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    ServerStateEntry entry = (ServerStateEntry) o;

    return Objects.equals(entry.serverId, this.serverId)
        && Objects.equals(entry.serverName, this.serverName)
        && Objects.equals(
            entry.lastHeartbeatTs.toEpochMilli(), this.lastHeartbeatTs.toEpochMilli());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.serverId, this.serverName, this.lastHeartbeatTs.toEpochMilli());
  }
}
