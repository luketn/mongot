# Rank Fusion

## Scenario

This scenario covers a query where mongod combines more than one search-producing branch. A client sends one aggregate command to mongod. mongod decomposes the search work and sends separate MongoT command messages for the text and vector branches. MongoT executes each branch through the relevant command path and returns batches that mongod can fuse.

## Example client operation

```java
collection.aggregate(List.of(
    new Document("$rankFusion",
        new Document("input",
            new Document("pipelines",
                new Document("text", List.of(
                    new Document("$search",
                        new Document("index", "default")
                            .append("text", new Document("path", "caption")
                                .append("query", "motorcycle")))))
                    .append("vector", List.of(
                        new Document("$vectorSearch",
                            new Document("index", "vector_caption")
                                .append("path", "captionEmbedding")
                                .append("queryVector", List.of(0.31, -0.11, 0.07))
                                .append("numCandidates", 50)
                                .append("limit", 20)))))))
            .append("combination", new Document("weights",
                new Document("text", 0.6).append("vector", 0.4)))),
    new Document("$limit", 10)
));
```

## Walkthrough

1. The client uses the same Java driver aggregate path as the simpler search stories: [MongoCollectionImpl.aggregate](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L331) to [AggregateOperationImpl.execute](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L196).
2. The driver sends one aggregate command to mongod. It does not know or connect to MongoT.
3. mongod parses `$rankFusion` in [DocumentSourceRankFusion::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/document_source_rank_fusion.cpp#L270).
4. [parseAndValidateRankedSelectionPipelines](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/document_source_rank_fusion.cpp#L216) validates that each input pipeline is ranked and selection-only.
5. [HybridSearchPipelineBuilder::constructDesugaredOutput](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/hybrid_search_pipeline_builder.cpp#L112) expands the input branches into ordinary pipeline stages, using `$unionWith` for later branches and final merge stages for fusion.
6. [RankFusionPipelineBuilder::buildInputPipelineDesugaringStages](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/rank_fusion_pipeline_builder.cpp#L383) adds rank and weighted reciprocal-rank score fields for each branch.
7. The text branch still invokes MongoT through [DocumentSourceSearch::desugar](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L166), [search_helpers::establishSearchCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L438), and [mongot_cursor::establishCursorsForSearchStage](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L225).
8. The vector branch invokes MongoT through [VectorSearchStage::doGetNext](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L138), [search_helpers::establishVectorSearchCursor](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L74), and [getRemoteCommandRequestForVectorSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L41).
9. The MongoT text branch uses [SearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), [MongotCursorManagerImpl.newCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150), and [LuceneSearchBatchProducer.execute](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170).
10. The MongoT vector branch uses [VectorSearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231), [LuceneVectorSearchManager.initialSearch](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L49), and [LuceneVectorSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchBatchProducer.java#L20).
11. MongoT returns branch-specific ids and scores. [RankFusionPipelineBuilder::buildScoreAndMergeStages](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/rank_fusion_pipeline_builder.cpp#L425) performs final score calculation, sorting, and removal of internal fusion fields inside mongod.

## MongoDB server classes involved

- [DocumentSourceRankFusion::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/document_source_rank_fusion.cpp#L270) parses the public `$rankFusion` stage.
- [parseAndValidateRankedSelectionPipelines](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/document_source_rank_fusion.cpp#L216) validates and parses the branch pipelines.
- [HybridSearchPipelineBuilder::constructDesugaredOutput](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/hybrid_search_pipeline_builder.cpp#L112) expands the hybrid search into executable aggregation stages.
- [RankFusionPipelineBuilder::buildInputPipelineDesugaringStages](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/rank_fusion_pipeline_builder.cpp#L383) adds branch rank and score calculation.
- [RankFusionPipelineBuilder::buildScoreAndMergeStages](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/rank_fusion_pipeline_builder.cpp#L425) computes the final fused score and sort order.
- [mongot_cursor::establishCursorsForSearchStage](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L225) invokes MongoT for `$search` branches.
- [search_helpers::establishVectorSearchCursor](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L74) invokes MongoT for `$vectorSearch` branches.
- [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) points each branch command at the configured MongoT address.

## MongoT classes involved

- [GrpcStreamingServer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L140) hosts the streams.
- [ServerCallHandler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) dispatches both branch commands.
- [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129) executes the text branch.
- [VectorSearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231) executes the vector branch.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) tracks branch cursor state.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above. `$rankFusion` can produce one text `search` stream, one vector `vectorSearch` stream, and MongoT `getMore` commands on the text stream. The vector command body is summarized in markdown by replacing the full `queryVector` field with `queryVectorSummary`.

### mongod -> MongoT: text branch

```json
{
  "search": "image",
  "collectionUUID": {
    "$binary": {
      "base64": "NDqH4tFIQ/aO6fC90aniKQ==",
      "subType": "04"
    }
  },
  "query": {
    "index": "default",
    "text": {
      "path": "caption",
      "query": "motorcycle"
    }
  },
  "cursorOptions": {
    "batchSize": 108
  },
  "$db": "clientDb"
}
```

### MongoT -> mongod: text branch first batch

```json
{
  "cursor": {
    "id": 1757498206156953394,
    "ns": "clientDb.documents",
    "nextBatch": [
      {
        "_id": 443005,
        "$searchScore": 2.9424455165863037
      },
      {
        "_id": 544713,
        "$searchScore": 2.9424455165863037
      },
      {
        "_id": 469139,
        "$searchScore": 2.7823147773742676
      },
      {
        "_id": 565387,
        "$searchScore": 2.2113354206085205
      }
    ]
  },
  "vars": {
    "SEARCH_META": {
      "count": {
        "lowerBound": 1458
      }
    }
  },
  "ok": 1
}
```

A first text batch can contain many documents. This example shows the first few and a later document from the same batch shape.

### mongod -> MongoT: text branch getMore

```json
{
  "getMore": 1757498206156953394,
  "collection": "image",
  "cursorOptions": {
    "batchSize": 162
  },
  "$db": "clientDb"
}
```

### mongod -> MongoT: vector branch command body summary

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
  "limit": 20,
  "$db": "clientDb"
}
```

### MongoT -> mongod: vector branch response

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
        "_id": 560943,
        "$vectorSearchScore": 0.8861563801765442
      }
    ]
  },
  "ok": 1
}
```

A vector branch batch can contain many documents. This example shows the first few and a later document from the same batch shape.
