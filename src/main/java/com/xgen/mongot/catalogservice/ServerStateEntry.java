package com.xgen.mongot.catalogservice;

import com.mongodb.client.model.Filters;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record ServerStateEntry(
    ObjectId serverId, String serverName, Instant lastHeartbeatTs, boolean shutdown)
    implements DocumentEncodable {

  private static final Duration EXPIRED_SERVER_DURATION = Duration.ofHours(2);

  public ServerStateEntry(ObjectId serverId, String serverName, Instant lastHeartbeatTs) {
    this(serverId, serverName, lastHeartbeatTs, false);
  }

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

    static final Field.WithDefault<Boolean> SHUTDOWN =
        Field.builder("shutdown").booleanField().optional().withDefault(false);
  }

  /**
   * Generates a doc to filter ServerStateEntries based on the unique server-id for the current
   * server.
   */
  public static BsonDocument serverIdFilter(ObjectId serverId) {
    return Filters.eq(Fields.ID.getName(), serverId).toBsonDocument();
  }

  /**
   * Filters for active servers defined by having heartbeated since the {@link
   * #EXPIRED_SERVER_DURATION} and not having the shutdown field set.
   */
  public static BsonDocument activeServersFilter() {
    return Filters.and(
            Filters.gt(
                Fields.LAST_HEARTBEAT_TS.getName(), Instant.now().minus(EXPIRED_SERVER_DURATION)),
            Filters.or(
                Filters.eq(Fields.SHUTDOWN.getName(), false),
                Filters.eq(Fields.SHUTDOWN.getName(), null)))
        .toBsonDocument();
  }

  /**
   * Filters for stale servers defined by having not heartbeated since the {@link
   * #EXPIRED_SERVER_DURATION}.
   */
  public static BsonDocument staleServerFilter() {
    return Filters.lt(
            Fields.LAST_HEARTBEAT_TS.getName(), Instant.now().minus(EXPIRED_SERVER_DURATION))
        .toBsonDocument();
  }

  public static ServerStateEntry fromBson(DocumentParser parser) throws BsonParseException {
    return new ServerStateEntry(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.SERVER_NAME).unwrap(),
        Instant.ofEpochMilli(parser.getField(Fields.LAST_HEARTBEAT_TS).unwrap().getValue()),
        parser.getField(Fields.SHUTDOWN).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.serverId)
        .field(Fields.SERVER_NAME, this.serverName)
        .field(Fields.LAST_HEARTBEAT_TS, new BsonDateTime(this.lastHeartbeatTs.toEpochMilli()))
        .field(Fields.SHUTDOWN, this.shutdown)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    ServerStateEntry entry = (ServerStateEntry) o;

    return Objects.equals(entry.serverId, this.serverId)
        && Objects.equals(entry.serverName, this.serverName)
        && Objects.equals(entry.lastHeartbeatTs.toEpochMilli(), this.lastHeartbeatTs.toEpochMilli())
        && Objects.equals(entry.shutdown, this.shutdown);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.serverId, this.serverName, this.lastHeartbeatTs.toEpochMilli(), this.shutdown);
  }
}
