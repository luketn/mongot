# Shutdown

## Scenario

MongoT shuts down by draining command streams, closing cursors and lifecycle services, stopping replication, updating server state, and releasing runtime resources. The client is not the source of this scenario. This represents process shutdown or controlled service termination.

## Example client operation

There is no client query in this scenario. Existing client commands may finish or fail depending on where they are in the shutdown drain, but the shutdown sequence itself is MongoT runtime work.

## Walkthrough

1. [DefaultLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/DefaultLifecycleManager.java#L296) closes runtime services.
2. [GrpcStreamingServer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L315) drains command streams so no new search work is accepted while active work is being resolved.
3. Cursor managers close active cursor state.
4. Replication services stop polling change streams.
5. [CommunityMetadataUpdater](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMetadataUpdater.java#L363) updates server state so metadata reflects shutdown.
6. MongoDB Java driver clients owned by MongoT close their sockets to mongod.
7. Logging, metrics, and tracing finish their last flushes.

## Driver path

Only MongoT-owned Java driver clients are involved. The application Java driver does not connect to MongoT during shutdown.

- [CommandBatchCursorHelper.getKillCursorsCommandDocument](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/CommandBatchCursorHelper.java#L72) shows the Java driver helper used when a cursor is killed.
- [InternalStreamConnection.sendAndReceive](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnection.java#L383) sends final metadata or cursor cleanup commands to mongod.

## MongoT classes involved

- [DefaultLifecycleManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/lifecycle/DefaultLifecycleManager.java#L296) closes services.
- [GrpcStreamingServer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L315) drains command streams.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L43) owns cursor cleanup.
- [MongoDbReplicationManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/MongoDbReplicationManager.java#L66) stops replication work.
- [ChangeStreamMongoCursorClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L98) owns change stream cursor state.
- [CommunityMetadataUpdater](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/config/provider/community/CommunityMetadataUpdater.java#L363) writes shutdown state.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above.

### MongoT -> mongod: update server state

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
          "state": "SHUTTING_DOWN",
          "lastHeartbeat": {
            "$date": "2026-04-27T00:00:00Z"
          }
        }
      },
      "upsert": false
    }
  ],
  "$db": "__mdb_internal_search"
}
```

### MongoT -> mongod: kill change stream cursor

```json
{
  "killCursors": "images",
  "cursors": [
    {
      "$numberLong": "842000001"
    }
  ],
  "$db": "sample_assets"
}
```

### MongoT -> mongod: final server state

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
          "state": "STOPPED",
          "shutdownComplete": true
        }
      },
      "upsert": false
    }
  ],
  "$db": "__mdb_internal_search"
}
```

## Accuracy note

Shutdown is not a query path. It documents the lifecycle endpoint after startup, ready state, query handling, replication, and steady-state operation.
