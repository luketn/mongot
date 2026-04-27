# Index Ready

## Scenario

After the basic runtime is created, MongoT makes the search indexes usable. It initializes metadata collections, reads the authoritative index catalog from mongod, starts index lifecycle managers, and establishes the low-volume replication streams that keep Lucene indexes current.

Replication, metrics, and tracing remain active after initialization. Those subsystems do not disappear once indexes are ready; they keep running while query scenarios execute.

## Walkthrough

1. [DefaultLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/DefaultLifecycleManager.java#L42) coordinates runtime services.
2. [CommunityMetadataUpdater](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMetadataUpdater.java#L114) initializes metadata indexes and server state.
3. [DefaultAuthoritativeIndexCatalog.listIndexes](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/catalogservice/DefaultAuthoritativeIndexCatalog.java#L82) reads search index definitions from the `__mdb_internal_search.indexCatalog` collection.
4. [IndexLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/IndexLifecycleManager.java#L41) brings individual index managers into a ready state.
5. [MongoDbReplicationManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/MongoDbReplicationManager.java#L66) prepares change-stream based replication.
6. [ChangeStreamMongoCursorClient.openCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L114) opens a change stream against mongod, usually with an initial empty batch.
7. [ChangeStreamMongoCursorClient.tryNext](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L150) continues polling with `getMore`.

## Example client operation

There is no application query in this scenario. The activity is MongoT setup and synchronization work.

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
        "serverId": 1
      },
      "name": "serverId_1",
      "background": true
    }
  ],
  "$db": "__mdb_internal_search"
}
```

### MongoT -> mongod: read server state

```json
{
  "find": "serverState",
  "filter": {
    "serverId": {
      "$oid": "66f100000000000000000001"
    }
  },
  "$db": "__mdb_internal_search"
}
```

### MongoT -> mongod: upsert server state

```json
{
  "update": "serverState",
  "updates": [
    {
      "q": {
        "serverId": {
          "$oid": "66f100000000000000000001"
        }
      },
      "u": {
        "$set": {
          "state": "STARTING",
          "host": "localhost:28000"
        }
      },
      "upsert": true
    }
  ],
  "$db": "__mdb_internal_search"
}
```

### MongoT -> mongod: read authoritative index catalog

```json
{
  "find": "indexCatalog",
  "filter": {
    "collectionUUID": {
      "$uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    }
  },
  "$db": "__mdb_internal_search"
}
```

### MongoT -> mongod: open change stream

```json
{
  "aggregate": "movies",
  "pipeline": [
    {
      "$changeStream": {
        "fullDocument": "updateLookup",
        "showMigrationEvents": true
      }
    },
    {
      "$addFields": {
        "_mdb_meta": "metadata fields added by MongoT"
      }
    }
  ],
  "cursor": {
    "batchSize": 0
  },
  "$db": "sample_mflix"
}
```

### MongoT -> mongod: poll change stream

```json
{
  "getMore": {
    "$numberLong": "842000001"
  },
  "collection": "movies",
  "batchSize": 1000,
  "maxTimeMS": 1000,
  "$db": "sample_mflix"
}
```

## Accuracy note

This scenario includes active communication with mongod. It is MongoT reading MongoDB metadata and opening replication cursors, not mongod sending a search command yet.
