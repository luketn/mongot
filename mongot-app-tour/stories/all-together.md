# Everything Together

## Scenario

This scenario summarizes the system in steady state: clients send ordinary MongoDB commands to mongod, mongod sends search work to MongoT, MongoT uses Lucene and cursor managers for search batches, replication keeps indexes current, and metrics and traces record the work.

## Example client operation

```java
collection.aggregate(List.of(
    new Document("$search",
        new Document("index", "default")
            .append("compound", new Document("must", List.of(
                new Document("text", new Document("path", "caption")
                    .append("query", "lunar mission")))))),
    new Document("$project",
        new Document("caption", 1)
            .append("licenseName", 1)
            .append("score", new Document("$meta", "searchScore")))
)).batchSize(20);
```

## Walkthrough

1. The client driver sends aggregate and, if iteration continues, `getMore` to mongod.
2. mongod owns aggregation, document fetch, rank fusion, and cursor continuation from the client perspective.
3. For `$search`, [DocumentSourceSearch::desugar](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L166), [search_helpers::establishSearchCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L438), and [mongot_cursor::establishCursorsForSearchStage](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L225) build the MongoT search cursor.
4. For `$vectorSearch`, [VectorSearchStage::doGetNext](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L138), [search_helpers::establishVectorSearchCursor](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L74), and [getRemoteCommandRequestForVectorSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L41) build the MongoT vector cursor.
5. For `$rankFusion`, [DocumentSourceRankFusion::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/document_source_rank_fusion.cpp#L270) and [HybridSearchPipelineBuilder::constructDesugaredOutput](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/hybrid_search_pipeline_builder.cpp#L112) expand the branches, then the search/vector stages above invoke MongoT independently.
6. For document fetch, [search_helpers::promoteStoredSourceOrAddIdLookup](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L709) inserts [DocumentSourceInternalSearchIdLookUp](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_internal_search_id_lookup.cpp#L65) when MongoT returns ids/scores instead of stored source fields.
7. [ServerCallHandler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L82) receives each MongoT command and dispatches to the relevant command class.
8. [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), [VectorSearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231), and [GetMoreCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/GetMoreCommand.java#L56) handle the query-side work.
9. [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L43) tracks active cursors.
10. [LuceneSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170) and [LuceneVectorSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchBatchProducer.java#L20) produce result batches.
11. [ChangeStreamMongoCursorClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L150), [IndexingWorkScheduler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java#L159), and [SingleLuceneIndexWriter](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332) keep indexes current in the background.
12. Metrics, logging, and traces are cross-cutting signals that are present across the runtime, but they are secondary to the command and replication paths.

## Driver path

The application Java driver still only speaks MongoDB wire protocol to mongod:

- [MongoCollectionImpl.aggregate](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L331) starts the aggregate call.
- [AggregateOperationImpl.execute](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L196) executes it as a read operation.
- [AggregateOperationImpl.getCommand](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L216) creates the aggregate command sent to mongod.
- [CommandBatchCursorHelper.getMoreCommandDocument](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/CommandBatchCursorHelper.java#L49) builds client `getMore` commands to mongod when iteration continues.

## MongoDB server path

- [mongot_cursor::getRemoteCommandRequestForSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L49) builds text search commands for MongoT.
- [getRemoteCommandRequestForVectorSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L41) builds vector search commands for MongoT.
- [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) points MongoT commands at the configured MongoT address.
- [mongot_cursor::establishCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L197) creates the `TaskExecutorCursor` used to call MongoT.
- [MongotTaskExecutorCursorGetMoreStrategy::createGetMoreRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor_getmore_strategy.cpp#L60) creates MongoT `getMore` commands when a MongoT cursor continues.

## MongoT classes involved

- [GrpcStreamingServer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L140)
- [ServerCallHandler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L82)
- [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129)
- [VectorSearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231)
- [GetMoreCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/GetMoreCommand.java#L56)
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L43)
- [MongoDbReplicationManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/MongoDbReplicationManager.java#L66)
- [ChangeStreamMongoCursorClient](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/replication/mongodb/common/ChangeStreamMongoCursorClient.java#L150)
- [SingleLuceneIndexWriter](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332)

## Command messages

These JSON documents use representative values. The command shapes match the code paths above.

### Client Java driver -> mongod: aggregate

```json
{
  "aggregate": "images",
  "pipeline": [
    {
      "$search": {
        "index": "default",
        "compound": {
          "must": [
            {
              "text": {
                "path": "caption",
                "query": "lunar mission"
              }
            }
          ]
        }
      }
    },
    {
      "$project": {
        "caption": 1,
        "licenseName": 1,
        "score": {
          "$meta": "searchScore"
        }
      }
    }
  ],
  "cursor": {
    "batchSize": 20
  },
  "$db": "sample_assets"
}
```

### mongod -> MongoT: search command

```json
{
  "search": "images",
  "$db": "sample_assets",
  "collectionUUID": {
    "$uuid": "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
  },
  "query": {
    "index": "default",
    "compound": {
      "must": [
        {
          "text": {
            "path": "caption",
            "query": "lunar mission"
          }
        }
      ]
    }
  },
  "cursorOptions": {
    "docsRequested": 20
  }
}
```

### MongoT -> mongod: search response

```json
{
  "cursor": {
    "id": {
      "$numberLong": "726493811486"
    },
    "ns": "sample_assets.images",
    "nextBatch": [
      {
        "_id": {
          "$oid": "66f100000000000000000060"
        },
        "score": 14.11
      }
    ]
  },
  "ok": 1
}
```

### MongoT -> mongod: background change stream getMore

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

### Client Java driver -> mongod: cursor getMore

```json
{
  "getMore": {
    "$numberLong": "551200001"
  },
  "collection": "images",
  "batchSize": 20,
  "$db": "sample_assets"
}
```

### mongod -> MongoT: search cursor getMore

```json
{
  "getMore": {
    "$numberLong": "726493811486"
  },
  "cursorOptions": {
    "docsRequested": 20
  }
}
```

## Accuracy note

This scenario is a steady-state summary, not a new independent workflow. It documents how command handling, cursor continuation, replication, and observability run together.
