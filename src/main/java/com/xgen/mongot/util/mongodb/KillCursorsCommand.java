package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.mongodb.serialization.KillCursorsCommandProxy;
import java.util.Collections;
import java.util.List;

public class KillCursorsCommand {

  private final String collection;
  private final List<Long> cursors;

  /** Constructs a new KillCursorsCommand for the supplied cursors. */
  public KillCursorsCommand(String collection, List<Long> cursors) {
    this.collection = collection;
    this.cursors = cursors;
  }

  /** Constructs a new KillCursorsCommand for the supplied cursor. */
  public KillCursorsCommand(String collection, long cursor) {
    this(collection, Collections.singletonList(cursor));
  }

  /** Returns a KillCursorsCommandProxy for the KillCursorsCommand. */
  public KillCursorsCommandProxy toProxy() {
    return new KillCursorsCommandProxy(this.collection, this.cursors);
  }
}
