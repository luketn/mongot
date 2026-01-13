package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.mongodb.serialization.RefreshSessionsCommandProxy;
import java.util.List;
import org.bson.BsonDocument;

public class RefreshSessionsCommand {

  private final List<BsonDocument> sessionIds;

  /** Constructs a new RefreshSessionsCommand. */
  public RefreshSessionsCommand(List<BsonDocument> sessionIds) {
    this.sessionIds = sessionIds;
  }

  /** Returns a RefreshSessionsCommandProxy for the GetMoreCommand. */
  public RefreshSessionsCommandProxy toProxy() {
    return new RefreshSessionsCommandProxy(this.sessionIds);
  }
}
