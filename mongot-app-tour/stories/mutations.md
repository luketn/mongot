# Inserts and Deletes

## Scenario

Writes go to mongod, not MongoT. MongoT observes the resulting collection changes through the replication/change-stream path that was established during initialization. Those changes are decoded, scheduled as indexing work, and applied to Lucene index writers.

The replication stream continues after startup, even when the workload is mostly reads.

## Example client operation

```java
collection.insertOne(new Document("_id", new ObjectId())
    .append("caption", "public observatory image")
    .append("licenseName", "CC BY"));

collection.deleteOne(Filters.eq("_id", deletedId));
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

These JSON documents use representative values. The command shapes match the code paths above.

### Client Java driver -> mongod: insert

```json
{
  "insert": "images",
  "documents": [
    {
      "_id": {
        "$oid": "66f100000000000000000050"
      },
      "caption": "public observatory image",
      "licenseName": "CC BY"
    }
  ],
  "$db": "sample_assets"
}
```

### Client Java driver -> mongod: delete

```json
{
  "delete": "images",
  "deletes": [
    {
      "q": {
        "_id": {
          "$oid": "66f100000000000000000050"
        }
      },
      "limit": 1
    }
  ],
  "$db": "sample_assets"
}
```

### MongoT -> mongod: change stream getMore

```json
{
  "getMore": {
    "$numberLong": "842000001"
  },
  "collection": "images",
  "batchSize": 1000,
  "maxTimeMS": 1000,
  "$db": "sample_assets"
}
```

### mongod -> MongoT: change event returned through the cursor

```json
{
  "_id": {
    "_data": "8266F100000000000001"
  },
  "operationType": "insert",
  "ns": {
    "db": "sample_assets",
    "coll": "images"
  },
  "documentKey": {
    "_id": {
      "$oid": "66f100000000000000000050"
    }
  },
  "fullDocument": {
    "_id": {
      "$oid": "66f100000000000000000050"
    },
    "caption": "public observatory image",
    "licenseName": "CC BY"
  }
}
```

### mongod -> MongoT: delete change event returned through the cursor

```json
{
  "_id": {
    "_data": "8266F100000000000002"
  },
  "operationType": "delete",
  "ns": {
    "db": "sample_assets",
    "coll": "images"
  },
  "documentKey": {
    "_id": {
      "$oid": "66f100000000000000000050"
    }
  }
}
```

## Accuracy note

MongoT is not on the client write path. It sees writes after mongod records them and exposes them through the change stream cursor.
