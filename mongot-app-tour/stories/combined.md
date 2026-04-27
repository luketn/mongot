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
                            .append("text", new Document("path", "plot")
                                .append("query", "mars habitat")))))
                    .append("vector", List.of(
                        new Document("$vectorSearch",
                            new Document("index", "plot_vector")
                                .append("path", "plotEmbedding")
                                .append("queryVector", List.of(0.31, -0.11, 0.07))
                                .append("numCandidates", 200)
                                .append("limit", 50)))))))
            .append("combination", new Document("weights",
                new Document("text", 0.6).append("vector", 0.4)))),
    new Document("$limit", 10)
));
```

## Walkthrough

1. The client uses the same Java driver aggregate path as the simpler search stories: [MongoCollectionImpl.aggregate](https://github.com/mongodb/mongo-java-driver/blob/main/driver-sync/src/main/com/mongodb/client/internal/MongoCollectionImpl.java#L331) to [AggregateOperationImpl.execute](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L196).
2. The driver sends one aggregate command to mongod. It does not know or connect to MongoT.
3. mongod owns rank fusion planning. It asks MongoT for each search-producing branch.
4. The text branch uses [SearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), [MongotCursorManagerImpl.newCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150), and [LuceneSearchBatchProducer.execute](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170).
5. The vector branch uses [VectorSearchCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231), [LuceneVectorSearchManager.initialSearch](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L49), and [LuceneVectorSearchBatchProducer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchBatchProducer.java#L20).
6. MongoT returns branch-specific ids and scores. mongod performs the rank fusion and final pipeline continuation.

## MongoT classes involved

- [GrpcStreamingServer](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java#L140) hosts the streams.
- [ServerCallHandler](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126) dispatches both branch commands.
- [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129) executes the text branch.
- [VectorSearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java#L231) executes the vector branch.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L150) tracks branch cursor state.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above.

### Client Java driver -> mongod: rank fusion aggregate

```json
{
  "aggregate": "movies",
  "pipeline": [
    {
      "$rankFusion": {
        "input": {
          "pipelines": {
            "text": [
              {
                "$search": {
                  "index": "default",
                  "text": {
                    "path": "plot",
                    "query": "mars habitat"
                  }
                }
              }
            ],
            "vector": [
              {
                "$vectorSearch": {
                  "index": "plot_vector",
                  "path": "plotEmbedding",
                  "queryVector": [0.31, -0.11, 0.07],
                  "numCandidates": 200,
                  "limit": 50
                }
              }
            ]
          }
        },
        "combination": {
          "weights": {
            "text": 0.6,
            "vector": 0.4
          }
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

### mongod -> MongoT: text branch

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
      "query": "mars habitat"
    }
  },
  "cursorOptions": {
    "docsRequested": 50
  }
}
```

### mongod -> MongoT: vector branch

```json
{
  "vectorSearch": "movies",
  "$db": "sample_mflix",
  "collectionUUID": {
    "$uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  },
  "index": "plot_vector",
  "path": "plotEmbedding",
  "queryVector": [0.31, -0.11, 0.07],
  "numCandidates": 200,
  "limit": 50,
  "cursorOptions": {
    "docsRequested": 50
  }
}
```

### MongoT -> mongod: branch batch response

```json
{
  "cursor": {
    "id": {
      "$numberLong": "726493811484"
    },
    "ns": "sample_mflix.movies",
    "nextBatch": [
      {
        "_id": {
          "$oid": "66f100000000000000000030"
        },
        "score": 7.81
      }
    ]
  },
  "ok": 1
}
```

## Accuracy note

Rank fusion is a mongod-owned composition step. MongoT runs branch searches; it is not the component that fuses the final ranking.
