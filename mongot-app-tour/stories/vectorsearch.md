# Vector Search

## Scenario

The client sends a vector search aggregate to mongod. The vector payload travels through the normal MongoDB Java driver command path to mongod. mongod then sends a `vectorSearch` command to MongoT over the gRPC command stream.

MongoT resolves the target Lucene index, converts the BSON vector query into internal vector search operators, runs vector candidate collection, and returns result ids and scores to mongod.

## Example client operation

```java
List<Double> embedding = List.of(0.12, -0.03, 0.44, 0.18);

collection.aggregate(List.of(
    new Document("$vectorSearch",
        new Document("index", "caption_vector")
            .append("path", "captionEmbedding")
            .append("queryVector", embedding)
            .append("numCandidates", 200)
            .append("limit", 10)),
    new Document("$project",
        new Document("caption", 1)
            .append("score", new Document("$meta", "vectorSearchScore")))
));
```

## Walkthrough

1. The client calls [MongoCollectionImpl.aggregate](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L331).
2. [AggregateOperationImpl.getCommand](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L216) builds a normal aggregate command. The Java driver does not special-case a MongoT connection.
3. mongod receives the aggregate and owns pipeline execution.
4. mongod sends a gRPC `vectorSearch` command body to MongoT.
5. [ServerCallHandler.handleMessage](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) dispatches the command to [VectorSearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231).
6. [VectorSearchOperator.fromBson](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/query/operators/VectorSearchOperator.java#L20) converts the BSON query shape into MongoT's operator model.
7. [LuceneVectorSearchManager.initialSearch](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L49) collects the vector candidates.
8. [LuceneVectorSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchBatchProducer.java#L20) serializes the prefetched vector results into batches.
9. [MongotCursorManagerImpl.newCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) registers cursor state if more results can be requested.

## MongoT classes involved

- [ServerCallHandler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) dispatches the command.
- [VectorSearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231) parses and validates vector search.
- [VectorSearchOperator](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/query/operators/VectorSearchOperator.java#L20) models the vector operator.
- [LuceneVectorSearchManager](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L49) runs the initial vector collection.
- [LuceneVectorSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchBatchProducer.java#L20) prepares vector search batches.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) tracks cursor state.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above.

### Client Java driver -> mongod: aggregate

```json
{
  "aggregate": "images",
  "pipeline": [
    {
      "$vectorSearch": {
        "index": "caption_vector",
        "path": "captionEmbedding",
        "queryVector": [0.12, -0.03, 0.44, 0.18],
        "numCandidates": 200,
        "limit": 10
      }
    },
    {
      "$project": {
        "caption": 1,
        "score": {
          "$meta": "vectorSearchScore"
        }
      }
    }
  ],
  "cursor": {},
  "$db": "sample_assets"
}
```

### mongod -> MongoT: gRPC vectorSearch command body

```json
{
  "vectorSearch": "images",
  "$db": "sample_assets",
  "collectionUUID": {
    "$uuid": "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
  },
  "index": "caption_vector",
  "path": "captionEmbedding",
  "queryVector": [0.12, -0.03, 0.44, 0.18],
  "numCandidates": 200,
  "limit": 10,
  "cursorOptions": {
    "docsRequested": 20
  }
}
```

### MongoT -> mongod: vector result batch

```json
{
  "cursor": {
    "id": {
      "$numberLong": "0"
    },
    "ns": "sample_assets.images",
    "nextBatch": [
      {
        "_id": {
          "$oid": "66f100000000000000000020"
        },
        "score": 0.9412
      }
    ]
  },
  "ok": 1
}
```

## Accuracy note

The Java driver sees this as an aggregate command sent to mongod. MongoT only sees the derived `vectorSearch` command sent by mongod.
