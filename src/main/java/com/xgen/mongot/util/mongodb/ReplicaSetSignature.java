package com.xgen.mongot.util.mongodb;

// A unique signature per replica set that is not reused upon shard removal and recreation.
public record ReplicaSetSignature(String rsId, String createDate) {
  public static ReplicaSetSignature fromSnapshotterBasePrefix(String basePrefix) {
    var parts = basePrefix.split("/");
    var rsUniqueId = parts[parts.length - 1];
    int separatorIdx = rsUniqueId.lastIndexOf('-');
    var rsId = rsUniqueId.substring(0, separatorIdx);
    var createDate = rsUniqueId.substring(separatorIdx + 1);
    return new ReplicaSetSignature(rsId, createDate);
  }
}
