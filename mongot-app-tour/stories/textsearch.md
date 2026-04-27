# Text Search

## Scenario

A generic application client uses the MongoDB Java driver to send an aggregate command to mongod. The client never opens a MongoT connection. When mongod reaches the `$search` stage, mongod sends a gRPC search command to MongoT. MongoT runs the Lucene search, creates or advances a MongoT cursor, and returns a batch back to mongod.

## Example client operation

```java
collection.aggregate(List.of(
    new Document("$search",
        new Document("index", "default")
            .append("text", new Document("path", "plot")
                .append("query", "space adventure"))),
    new Document("$project",
        new Document("title", 1)
            .append("score", new Document("$meta", "searchScore"))),
    new Document("$limit", 10)
));
```

## Walkthrough

1. The client calls [MongoCollectionImpl.aggregate](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L331).
2. The sync API creates [AggregateIterableImpl](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/AggregateIterableImpl.java#L207).
3. [AggregateIterableImpl.asAggregateOperation](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/AggregateIterableImpl.java#L231) builds an aggregate read operation.
4. [AggregateOperationImpl.execute](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L196) executes the operation against mongod.
5. [AggregateOperationImpl.getCommand](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L216) builds the BSON aggregate command.
6. The driver selects a MongoDB server with [ClusterBinding.getReadConnectionSource](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/binding/ClusterBinding.java#L67). That server is mongod, not MongoT.
7. mongod owns aggregation execution. When the pipeline reaches `$search`, mongod opens or reuses a gRPC command stream to MongoT.
8. MongoT receives the message in [ServerCallHandler.onNext](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L82).
9. [ServerCallHandler.handleMessage](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) parses the command and dispatches to [SearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129).
10. [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L188) validates cursor options, resolves index state, and creates the first batch.
11. [MongotCursorManagerImpl.newCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) creates a cursor for continuation.
12. [LuceneSearchBatchProducer.execute](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170) runs the Lucene collection work and returns results.

## MongoT classes involved

- [GrpcStreamingServer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L140) owns the command stream.
- [ServerCallHandler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L82) receives and dispatches commands.
- [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129) handles the search command.
- [DefaultAuthoritativeIndexCatalog](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/catalogservice/DefaultAuthoritativeIndexCatalog.java#L82) resolves the target index.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) creates the MongoT cursor.
- [IndexCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L83) registers the index-local cursor.
- [MongotCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90) advances batches.
- [LuceneSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170) performs the Lucene batch work.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above.

### Client Java driver -> mongod: aggregate

```json
{
  "aggregate": "movies",
  "pipeline": [
    {
      "$search": {
        "index": "default",
        "text": {
          "path": "plot",
          "query": "space adventure"
        }
      }
    },
    {
      "$project": {
        "title": 1,
        "score": {
          "$meta": "searchScore"
        }
      }
    },
    {
      "$limit": 10
    }
  ],
  "cursor": {},
  "$db": "sample_mflix"
}
```

### mongod -> MongoT: gRPC search command body

```json
{
  "search": "movies",
  "$db": "sample_mflix",
  "collectionUUID": {
    "$uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  },
  "query": {
    "index": "default",
    "text": {
      "path": "plot",
      "query": "space adventure"
    }
  },
  "cursorOptions": {
    "docsRequested": 20
  }
}
```

### MongoT -> mongod: first search batch

```json
{
  "cursor": {
    "id": {
      "$numberLong": "726493811483"
    },
    "ns": "sample_mflix.movies",
    "nextBatch": [
      {
        "_id": {
          "$oid": "66f100000000000000000010"
        },
        "score": 12.34
      }
    ]
  },
  "vars": {
    "SEARCH_META": {
      "count": {
        "lowerBound": 1
      }
    }
  },
  "ok": 1
}
```

## Accuracy note

The client driver connects to mongod only. The first MongoT-facing command is created by mongod after aggregation planning reaches `$search`.
