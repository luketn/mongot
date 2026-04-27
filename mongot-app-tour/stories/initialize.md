# Index Ready

## Scenario

After the basic runtime is created, MongoT makes the search indexes usable. It initializes metadata collections, reads the authoritative index catalog from mongod, starts index lifecycle managers, and establishes the low-volume replication streams that keep Lucene indexes current.

Replication, metrics, and tracing remain active after initialization and keep running while query scenarios execute.

## Walkthrough

1. [DefaultLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/DefaultLifecycleManager.java#L42) coordinates runtime services.
2. [CommunityMetadataUpdater](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMetadataUpdater.java#L114) initializes metadata indexes and server state.
3. [DefaultAuthoritativeIndexCatalog.listIndexes](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/catalogservice/DefaultAuthoritativeIndexCatalog.java#L82) reads search index definitions from the `__mdb_internal_search.indexCatalog` collection.
4. [IndexLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/IndexLifecycleManager.java#L41) brings individual index managers into a ready state.
5. [MongoDbReplicationManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/MongoDbReplicationManager.java#L66) prepares change-stream based replication.
6. [ChangeStreamMongoCursorClient.openCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L114) opens a change stream against mongod, usually with an initial empty batch.
7. [ChangeStreamMongoCursorClient.tryNext](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L150) continues polling with `getMore`.

## Driver path

MongoT again uses the Java driver as an internal client:

- [CommandProtocolImpl.execute](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/CommandProtocolImpl.java#L57) sends metadata commands to mongod.
- [InternalStreamConnection.sendAndReceive](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnection.java#L383) performs the socket exchange.
- [CommandBatchCursorHelper.getMoreCommandDocument](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/CommandBatchCursorHelper.java#L49) is the Java driver helper that builds `getMore` commands for cursor continuation.

## MongoT classes involved

- [CommunityMetadataUpdater](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMetadataUpdater.java#L114) creates metadata indexes and initializes server state.
- [DefaultAuthoritativeIndexCatalog](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/catalogservice/DefaultAuthoritativeIndexCatalog.java#L82) reads configured search indexes.
- [MetadataClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/catalogservice/MetadataClient.java#L160) issues catalog collection reads.
- [DefaultLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/DefaultLifecycleManager.java#L42) sequences service startup.
- [IndexLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/IndexLifecycleManager.java#L41) manages per-index lifecycle.
- [MongoDbReplicationManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/MongoDbReplicationManager.java#L66) owns change stream replication.
- [ChangeStreamAggregateCommandFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamAggregateCommandFactory.java#L39) builds the aggregate command for the change stream.
- [ChangeStreamMongoCursorClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L98) opens and advances the change stream cursor.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above.

### MongoT -> mongod: create metadata index

```json
{
  "createIndexes": "indexStats",
  "indexes": [
    {
      "key": {
        "_id.serverId": 1
      },
      "name": "_id.serverId_1",
      "background": true
    }
  ],
  "$db": "__mdb_internal_search"
}
```

### mongod -> MongoT: create metadata index response

```json
{
  "numIndexesBefore": 3,
  "numIndexesAfter": 3,
  "commitQuorum": "votingMembers",
  "note": "all indexes already exist",
  "ok": 1.0
}
```

### MongoT -> mongod: read server state

```json
{
  "find": "serverState",
  "readConcern": {
    "level": "majority"
  },
  "filter": {
    "_id": {
      "$oid": "69ec2d86a722ed3e1957e639"
    }
  },
  "$db": "__mdb_internal_search"
}
```

### mongod -> MongoT: read server state response

```json
{
  "cursor": {
    "id": 0,
    "ns": "__mdb_internal_search.serverState",
    "firstBatch": [
      {
        "_id": {
          "$oid": "69ec2d86a722ed3e1957e639"
        },
        "serverName": "69ec2d86a722ed3e1957e639",
        "lastHeartbeatTs": {
          "$date": "2026-04-27T03:28:29.065Z"
        },
        "ready": false,
        "shutdown": false
      }
    ]
  },
  "ok": 1.0
}
```

### MongoT -> mongod: upsert server state

```json
{
  "update": "serverState",
  "ordered": true,
  "txnNumber": 1,
  "updates": [
    {
      "q": {
        "_id": {
          "$oid": "69ec2d86a722ed3e1957e639"
        }
      },
      "u": {
        "_id": {
          "$oid": "69ec2d86a722ed3e1957e639"
        },
        "serverName": "69ec2d86a722ed3e1957e639",
        "lastHeartbeatTs": {
          "$date": "2026-04-27T03:47:29.064Z"
        },
        "ready": false,
        "shutdown": false
      },
      "upsert": true
    }
  ],
  "$db": "__mdb_internal_search"
}
```

### mongod -> MongoT: upsert server state response

```json
{
  "n": 1,
  "nModified": 1,
  "ok": 1.0
}
```

### MongoT -> mongod: read authoritative index catalog

```json
{
  "find": "indexCatalog",
  "readConcern": {
    "level": "majority"
  },
  "filter": {
  },
  "$db": "__mdb_internal_search"
}
```

The authoritative index catalog can contain multiple entries, including indexes like `clientDb.documents/default` and `clientDb.documents/vector_caption`.

### MongoT -> mongod: open change stream

```json
{
  "aggregate": "image",
  "readConcern": {
    "level": "majority"
  },
  "pipeline": [
    {
      "$changeStream": {
        "fullDocument": "updateLookup",
        "showMigrationEvents": true,
        "startAfter": {
          "_data": "8269EED7BC000000012B0429296E1404"
        }
      }
    },
    {
      "$addFields": {
        "fullDocument.69ec2f48a722ed3e1957e64d._id": "$fullDocument._id"
      }
    }
  ],
  "maxTimeMS": 1000,
  "cursor": {
    "batchSize": 0
  },
  "$db": "clientDb"
}
```

### mongod -> MongoT: change stream open response

```json
{
  "cursor": {
    "firstBatch": [],
    "postBatchResumeToken": {
      "_data": "8269EED7BC000000012B0429296E1404"
    },
    "id": 131035085312391959,
    "ns": "clientDb.documents"
  },
  "ok": 1.0
}
```

### MongoT -> mongod: poll change stream

```json
{
  "getMore": 131035085312391959,
  "collection": "image",
  "$db": "clientDb"
}
```

### mongod -> MongoT: empty change stream poll response

```json
{
  "cursor": {
    "nextBatch": [],
    "postBatchResumeToken": {
      "_data": "8269EEDC51000000022B0429296E1404"
    },
    "id": 131035085312391959,
    "ns": "clientDb.documents"
  },
  "ok": 1.0
}
```
