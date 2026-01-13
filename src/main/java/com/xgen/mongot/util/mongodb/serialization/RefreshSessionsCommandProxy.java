package com.xgen.mongot.util.mongodb.serialization;

import com.xgen.mongot.util.Check;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

/**
 * RefreshSessionsCommandProxy is a proxy for a <em>refreshSessions</em> command.
 *
 * <p>See https://docs.mongodb.com/manual/reference/command/refreshSessions/#dbcmd.refreshSessions
 */
public class RefreshSessionsCommandProxy implements Bson {

  private static final String REFRESH_SESSIONS_FIELD = "refreshSessions";

  private final List<BsonDocument> sessionIds;

  /** Constructs a new RefreshSessionsCommandProxy. */
  public RefreshSessionsCommandProxy(List<BsonDocument> sessionIds) {
    this.sessionIds = sessionIds;
  }

  @Override
  public <T> BsonDocument toBsonDocument(Class<T> documentClass, CodecRegistry codecRegistry) {
    Check.argNotNull(documentClass, "documentClass");
    Check.argNotNull(codecRegistry, "codecRegistry");

    return new BsonDocument().append(REFRESH_SESSIONS_FIELD, new BsonArray(this.sessionIds));
  }
}
