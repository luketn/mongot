# Writes and Change Streams

## Scenario

Writes go to mongod, not MongoT. MongoT observes the resulting insert, update, and delete events through the replication/change-stream path that was established during initialization. Those changes are decoded, scheduled as indexing work, and applied to Lucene index writers.

The replication stream continues after startup, even when the workload is mostly reads.

## Example client operation

```javascript
await collection.insertOne({
  caption: "public observatory image",
  licenseName: "CC BY"
});

await collection.updateOne(
  { caption: "public observatory image" },
  { "$set": { caption: "updated caption" } }
);

await collection.deleteOne({ caption: "updated caption" });
```

## Walkthrough

1. The client sends ordinary write commands to mongod. These commands do not target MongoT.
2. MongoT's change stream cursor, opened earlier by [ChangeStreamMongoCursorClient.openCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L114), continues polling mongod.
3. [ChangeStreamMongoCursorClient.tryNext](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L150) reads change events and tracks resume state.
4. [IndexingWorkScheduler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java#L159) schedules indexing work.
5. [SingleLuceneIndexWriter.updateIndex](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332) applies insert, update, and delete changes.
6. [SingleLuceneIndexWriter.deleteDocument](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L549) removes deleted documents from Lucene.
7. [SingleLuceneIndexWriter.commit](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L349) commits index writer changes.

## Driver path

The application Java driver talks to mongod. MongoT's own Java driver connection polls change streams from mongod.

- [MongoCollectionImpl.insertOne](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L475) creates the client insert operation.
- [MongoCollectionImpl.deleteOne](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L533) creates the client delete operation.
- [MixedBulkWriteOperation.execute](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/MixedBulkWriteOperation.java#L189) executes the write against the selected mongod.
- [InternalStreamConnection.sendAndReceive](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/connection/InternalStreamConnection.java#L383) sends client write commands to mongod.
- [CommandBatchCursorHelper.getMoreCommandDocument](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/CommandBatchCursorHelper.java#L49) builds MongoT's change stream `getMore` commands through its internal driver client.

## MongoT classes involved

- [MongoDbReplicationManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/MongoDbReplicationManager.java#L66) owns replication.
- [ChangeStreamAggregateCommandFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamAggregateCommandFactory.java#L39) creates the change stream aggregate.
- [ChangeStreamMongoCursorClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L150) reads change events.
- [IndexingWorkScheduler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java#L159) schedules index updates.
- [SingleLuceneIndexWriter](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332) applies Lucene writes and deletes.

## Command messages

These examples use real message field names. Long arrays may be shortened with ellipses; no replacement fields are introduced. They show MongoT's change stream cursor traffic after client writes have already gone to mongod.

### MongoT -> mongod: change stream getMore

```json
{
  "getMore": 131035085312391959,
  "collection": "image",
  "$db": "clientDb"
}
```

### mongod -> MongoT: insert change event returned through the cursor

```json
{
  "_id": {
    "_data": "8269EEDD0D000000012B042C0100296E5A1004343A87E2D14843F68EE9F0BDD1A9E229463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B657900463C5F6964003C636F6465782D636170747572652D3137373732363138333737313200000004"
  },
  "operationType": "insert",
  "clusterTime": {
    "$timestamp": {
      "t": 1777261837,
      "i": 1
    }
  },
  "wallTime": {
    "$date": "2026-04-27T03:50:37.72Z"
  },
  "ns": {
    "db": "clientDb",
    "coll": "image"
  },
  "documentKey": {
    "_id": "example-write-001"
  },
  "fullDocument": {
    "_id": "example-write-001",
    "caption": "codex temporary motorcycle document",
    "url": "https://example.invalid/codex",
    "width": 1,
    "height": 1,
    "dateCaptured": {
      "$date": "2026-04-27T03:50:37.712Z"
    },
    "hasPerson": false,
    "licenseName": "Diagnostic License",
    "licenseUrl": "https://example.invalid/license",
    "69ec2f48a722ed3e1957e64d": {
      "_id": "example-write-001"
    }
  }
}
```

### mongod -> MongoT: update change event returned through the cursor

```json
{
  "operationType": "update",
  "clusterTime": {
    "$timestamp": {
      "t": 1777261839,
      "i": 1
    }
  },
  "ns": {
    "db": "clientDb",
    "coll": "image"
  },
  "documentKey": {
    "_id": "example-write-001"
  },
  "fullDocument": {
    "_id": "example-write-001",
    "caption": "codex temporary motorcycle document updated",
    "url": "https://example.invalid/codex",
    "width": 1,
    "height": 1,
    "dateCaptured": {
      "$date": "2026-04-27T03:50:37.712Z"
    },
    "hasPerson": true,
    "licenseName": "Diagnostic License",
    "licenseUrl": "https://example.invalid/license"
  },
  "updateDescription": {
    "updatedFields": {
      "caption": "codex temporary motorcycle document updated",
      "hasPerson": true
    },
    "removedFields": [],
    "truncatedArrays": []
  }
}
```

### mongod -> MongoT: delete change event returned through the cursor

```json
{
  "_id": {
    "_data": "8269EEDD10000000022B042C0100296E5A1004343A87E2D14843F68EE9F0BDD1A9E229463C6F7065726174696F6E54797065003C64656C6574650046646F63756D656E744B657900463C5F6964003C636F6465782D636170747572652D3137373732363138333737313200000004"
  },
  "operationType": "delete",
  "clusterTime": {
    "$timestamp": {
      "t": 1777261840,
      "i": 2
    }
  },
  "ns": {
    "db": "clientDb",
    "coll": "image"
  },
  "documentKey": {
    "_id": "example-write-001"
  }
}
```
