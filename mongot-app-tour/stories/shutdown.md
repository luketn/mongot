# Shutdown

## Scenario

MongoT shuts down by draining command streams, closing cursors and lifecycle services, stopping replication, updating server state, and releasing runtime resources. This represents process shutdown or controlled service termination.

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

These JSON documents use representative values. The command shapes match the code paths above. Shutdown produces final heartbeat, metadata, change-stream cleanup, and session cleanup messages; the entries below show the important shapes.

### MongoT -> mongod: update server state

```json
{
  "update": "serverState",
  "ordered": true,
  "txnNumber": 58,
  "updates": [
    {
      "q": {
        "_id": {
          "$oid": "69ec2d86a722ed3e1957e639"
        }
      },
      "u": {
        "$set": {
          "shutdown": true
        }
      }
    }
  ],
  "$db": "__mdb_internal_search"
}
```

### mongod -> MongoT: shutdown state response

```json
{
  "n": 1,
  "nModified": 1,
  "ok": 1.0
}
```

### MongoT -> mongod: kill change stream cursor

```json
{
  "killCursors": "image",
  "cursors": [
    131035085312391959
  ],
  "$db": "clientDb"
}
```

### mongod -> MongoT: killCursors response

```json
{
  "cursorsKilled": [
    131035085312391959
  ],
  "cursorsNotFound": [],
  "cursorsAlive": [],
  "cursorsUnknown": [],
  "ok": 1.0
}
```

### MongoT -> mongod: end sessions

```json
{
  "endSessions": [
    {
      "id": {
        "$binary": {
          "base64": "RzQ6zVLlTmGXu5EonbG/mQ==",
          "subType": "04"
        }
      }
    }
  ],
  "$db": "admin"
}
```

### mongod -> MongoT: end sessions response

```json
{
  "ok": 1.0
}
```
