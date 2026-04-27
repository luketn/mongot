# Vector Search

## Scenario

The client sends a vector search aggregate to mongod. The vector payload travels through the normal MongoDB Java driver command path to mongod. mongod parses `$vectorSearch`, creates the vector search execution stage, and then sends a `vectorSearch` command to MongoT over the MongoT command stream.

MongoT resolves the target Lucene index, converts the BSON vector query into internal vector search operators, runs vector candidate collection, and returns result ids and scores to mongod.

## Example client operation

```java
List<Double> embedding = List.of(0.12, -0.03, 0.44, 0.18);

collection.aggregate(List.of(
    new Document("$vectorSearch",
        new Document("index", "vector_caption")
            .append("path", "captionEmbedding")
            .append("queryVector", embedding)
            .append("numCandidates", 50)
            .append("limit", 25)),
    new Document("$project",
        new Document("caption", 1)
            .append("score", new Document("$meta", "vectorSearchScore")))
));
```

## Walkthrough

1. The client calls [MongoCollectionImpl.aggregate](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L331).
2. [AggregateOperationImpl.getCommand](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L216) builds a normal aggregate command. The Java driver does not special-case a MongoT connection.
3. mongod parses `$vectorSearch` in [DocumentSourceVectorSearch::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_vector_search.cpp#L177), preserving fields that MongoT may understand even if mongod treats them as pass-through query content.
4. [documentSourceVectorSearchToStageFn](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L39) converts the pipeline source into the executable [VectorSearchStage](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L80).
5. On first execution, [VectorSearchStage::doGetNext](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L138) calls [search_helpers::establishVectorSearchCursor](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L74).
6. [getRemoteCommandRequestForVectorSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L41) builds the `vectorSearch` command body, [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) targets the configured MongoT address, and [mongot_cursor::establishCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L197) creates the MongoT `TaskExecutorCursor`.
7. [ServerCallHandler.handleMessage](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) dispatches the command to [VectorSearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231).
8. [VectorSearchOperator.fromBson](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/query/operators/VectorSearchOperator.java#L20) converts the BSON query shape into MongoT's operator model.
9. [LuceneVectorSearchManager.initialSearch](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L49) collects the vector candidates.
10. [LuceneVectorSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchBatchProducer.java#L20) serializes the prefetched vector results into batches.
11. [MongotCursorManagerImpl.newCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) registers cursor state if more results can be requested.

## MongoDB server classes involved

- [DocumentSourceVectorSearch::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_vector_search.cpp#L177) parses the public `$vectorSearch` stage.
- [DocumentSourceVectorSearch::desugar](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_vector_search.cpp#L208) appends stored-source or id-lookup handling when needed.
- [documentSourceVectorSearchToStageFn](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L39) creates the executable vector search stage.
- [VectorSearchStage::doGetNext](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L138) establishes the MongoT cursor on first execution.
- [search_helpers::establishVectorSearchCursor](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L74) creates the vector search remote cursor.
- [getRemoteCommandRequestForVectorSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L41) builds the MongoT `vectorSearch` command body.
- [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) points the command at the configured MongoT address.
- [mongot_cursor::establishCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L197) creates the `TaskExecutorCursor` that invokes MongoT.

## MongoT classes involved

- [ServerCallHandler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) dispatches the command.
- [VectorSearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231) parses and validates vector search.
- [VectorSearchOperator](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/query/operators/VectorSearchOperator.java#L20) models the vector operator.
- [LuceneVectorSearchManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L49) runs the initial vector collection.
- [LuceneVectorSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchBatchProducer.java#L20) prepares vector search batches.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) tracks cursor state.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above. The vector command body is summarized in markdown by replacing the full vector with `queryVectorSummary`.

### mongod -> MongoT: gRPC vectorSearch command body summary

```json
{
  "vectorSearch": "image",
  "collectionUUID": {
    "$binary": {
      "base64": "NDqH4tFIQ/aO6fC90aniKQ==",
      "subType": "04"
    }
  },
  "index": "vector_caption",
  "path": "captionEmbedding",
  "queryVectorSummary": {
    "length": 768,
    "first8": [
      -0.018413003534078598,
      0.039294157177209854,
      -0.1552632749080658,
      0.028887825086712837,
      0.022704750299453735,
      0.05024779215455055,
      0.019792508333921432,
      0.012846584431827068
    ],
    "last3": [
      0.017677120864391327,
      -0.044313110411167145,
      -0.008208148181438446
    ]
  },
  "numCandidates": 50,
  "limit": 25,
  "$db": "clientDb"
}
```

### MongoT -> mongod: vector result batch

```json
{
  "cursor": {
    "id": 0,
    "ns": "clientDb.documents",
    "nextBatch": [
      {
        "_id": 391895,
        "$vectorSearchScore": 1.0
      },
      {
        "_id": 382554,
        "$vectorSearchScore": 0.9136942625045776
      },
      {
        "_id": 334699,
        "$vectorSearchScore": 0.9083335995674133
      },
      {
        "_id": 423875,
        "$vectorSearchScore": 0.8838289976119995
      }
    ]
  },
  "ok": 1
}
```

A `nextBatch` can contain many documents. This example shows the first few and a later document from the same batch shape.
