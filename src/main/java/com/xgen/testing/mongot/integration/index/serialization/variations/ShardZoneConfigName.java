package com.xgen.testing.mongot.integration.index.serialization.variations;

public enum ShardZoneConfigName {
  DOCS_ALL_SHARDS("docsAllShards"),
  DOCS_SOME_SHARDS("docsSomeShards"),
  DOCS_ONE_SHARD("docsOneShard");

  private final String configName;

  ShardZoneConfigName(String configName) {
    this.configName = configName;
  }

  public String getConfigName() {
    return this.configName;
  }

  public static ShardZoneConfigName fromConfigName(String configName) {
    for (ShardZoneConfigName shardZoneConfigName : ShardZoneConfigName.values()) {
      if (shardZoneConfigName.getConfigName().equals(configName)) {
        return shardZoneConfigName;
      }
    }
    return null;
  }
}
