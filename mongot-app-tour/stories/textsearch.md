# Text Search

## Scenario

A generic application client uses the MongoDB Java driver to send an aggregate command to mongod. The client never opens a MongoT connection. When mongod reaches the `$search` stage, mongod parses and desugars the stage into MongoDB server search internals, then opens the MongoT remote command cursor. MongoT runs the Lucene search, creates or advances a MongoT cursor, and returns a batch back to mongod.

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
7. mongod parses `$search` in [DocumentSourceSearch::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L128).
8. During pipeline rewriting, [DocumentSourceSearch::desugar](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L166) creates the internal [DocumentSourceInternalSearchMongotRemote](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_internal_search_mongot_remote.cpp#L63) stage and wires stored-source or id-lookup handling.
9. When execution needs the remote search cursor, [search_helpers::establishSearchCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L438) calls [mongot_cursor::establishCursorsForSearchStage](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L225).
10. [mongot_cursor::getRemoteCommandRequestForSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L49) builds the BSON `search` command body, [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) targets `getMongotAddress()`, and [mongot_cursor::establishCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L197) wraps it in a `TaskExecutorCursor`. This is the mongod C++ code path that invokes MongoT.
11. MongoT receives the message in [ServerCallHandler.onNext](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L82).
12. [ServerCallHandler.handleMessage](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) parses the command and dispatches to [SearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129).
13. [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L188) validates cursor options, resolves index state, and creates the first batch.
14. [MongotCursorManagerImpl.newCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) creates a cursor for continuation.
15. [LuceneSearchBatchProducer.execute](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170) runs the Lucene collection work and returns results.

## MongoDB server classes involved

- [DocumentSourceSearch::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L128) parses the public `$search` stage.
- [DocumentSourceSearch::desugar](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L166) rewrites `$search` into internal execution stages.
- [DocumentSourceInternalSearchMongotRemote](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_internal_search_mongot_remote.cpp#L63) represents the remote MongoT-producing stage in the aggregation pipeline.
- [search_helpers::establishSearchCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L438) establishes MongoT cursors for classic pipeline execution.
- [search_helpers::establishSearchCursorsSBE](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L504) establishes MongoT cursors for the SBE path.
- [mongot_cursor::getRemoteCommandRequestForSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L49) builds the MongoT `search` command body.
- [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) points the command at the configured MongoT address.
- [mongot_cursor::establishCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L197) creates the `TaskExecutorCursor` that invokes MongoT.

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
