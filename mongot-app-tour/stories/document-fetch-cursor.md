# Document Fetch and getMore

## Scenario

This scenario covers two related behaviors that happen after the first MongoT search batch.

First, if the client asks for fields that are not returned from Lucene stored source fields, MongoT can return ids and scores while mongod fetches the matched source documents from MongoDB storage. That document fetch is inside mongod's execution plan. It is not the Java driver issuing a second query.

Second, if the client consumes more results than the first batch contains, the Java driver sends `getMore` to mongod. mongod then sends its own `getMore` command to MongoT to continue the MongoT cursor.

## Example client operation

```java
AggregateIterable<Document> results = collection.aggregate(List.of(
    new Document("$search",
        new Document("index", "default")
            .append("text", new Document("path", "caption")
                .append("query", "public domain observatory"))),
    new Document("$project",
        new Document("caption", 1)
            .append("licenseName", 1)
            .append("licenseUrl", 1)
            .append("score", new Document("$meta", "searchScore")))
)).batchSize(20);

for (Document result : results) {
    // Iteration may cause driver getMore commands to mongod.
}
```

## Walkthrough

1. The client sends one aggregate command to mongod through [AggregateOperationImpl.getCommand](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L216).
2. mongod sends a gRPC `search` command to MongoT.
3. [ProjectFactory.build](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/ProjectFactory.java#L25) chooses how MongoT should shape returned fields.
4. If stored source is not being returned, [IdLookupFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/IdLookupFactory.java#L13) returns id-oriented documents. This lets mongod fetch requested fields from the matched collection documents.
5. [SearchCommand.getBatch](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317) prepares the batch and cursor response.
6. The Java driver receives a cursor from mongod. If the client continues iteration, [CommandBatchCursorHelper.getMoreCommandDocument](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/CommandBatchCursorHelper.java#L49) builds a `getMore` command to mongod.
7. mongod advances its pipeline cursor and sends a MongoT `getMore` command over the gRPC stream.
8. [GetMoreCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/GetMoreCommand.java#L56) asks [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L43) for the next batch.
9. [MongotCursor.getNextBatch](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90) advances the batch producer.

## MongoT classes involved

- [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317) builds the first batch and cursor response.
- [ProjectFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/ProjectFactory.java#L25) chooses stored-source or id lookup behavior.
- [IdLookupFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/IdLookupFactory.java#L13) builds id-based projection output.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L43) finds and owns cursor state.
- [GetMoreCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/GetMoreCommand.java#L56) handles MongoT cursor continuation.
- [MongotCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90) obtains the next batch from the producer.

## Command messages

These JSON documents use representative values. The command shapes match the code paths above.

### Client Java driver -> mongod: aggregate requesting non-stored fields

```json
{
  "aggregate": "images",
  "pipeline": [
    {
      "$search": {
        "index": "default",
        "text": {
          "path": "caption",
          "query": "public domain observatory"
        }
      }
    },
    {
      "$project": {
        "caption": 1,
        "licenseName": 1,
        "licenseUrl": 1,
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
    "text": {
      "path": "caption",
      "query": "public domain observatory"
    }
  },
  "cursorOptions": {
    "docsRequested": 20
  }
}
```

### MongoT -> mongod: id-oriented search batch

```json
{
  "cursor": {
    "id": {
      "$numberLong": "726493811485"
    },
    "ns": "sample_assets.images",
    "nextBatch": [
      {
        "_id": {
          "$oid": "66f100000000000000000040"
        },
        "score": 10.52
      }
    ]
  },
  "ok": 1
}
```

### mongod internal document fetch

```json
{
  "internalPlan": "fetch source collection documents by RecordId or _id for MongoT matches",
  "collection": "sample_assets.images",
  "fieldsNeededByClient": ["caption", "licenseName", "licenseUrl"],
  "notAClientDriverCommand": true
}
```

### Client Java driver -> mongod: getMore

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

### mongod -> MongoT: gRPC getMore command body

```json
{
  "getMore": {
    "$numberLong": "726493811485"
  },
  "cursorOptions": {
    "docsRequested": 20
  }
}
```

### MongoT -> mongod: next search batch

```json
{
  "cursor": {
    "id": {
      "$numberLong": "726493811485"
    },
    "ns": "sample_assets.images",
    "nextBatch": [
      {
        "_id": {
          "$oid": "66f100000000000000000041"
        },
        "score": 9.88
      }
    ]
  },
  "ok": 1
}
```

## Accuracy note

There is no Java driver second query to MongoT or MongoDB for the projected fields. The client driver sends aggregate and later `getMore` to mongod. mongod performs any required source document fetch as part of its own execution.
