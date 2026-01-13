package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.mongodb.serialization.CollectionStatsCommandProxy;

public class CollectionStatsCommand {

  private final String collection;

  public CollectionStatsCommand(String collection) {
    this.collection = collection;
  }

  public CollectionStatsCommandProxy toProxy() {
    return new CollectionStatsCommandProxy(this.collection);
  }
}
