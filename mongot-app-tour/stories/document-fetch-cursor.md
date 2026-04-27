# Document Fetch

## Scenario

This scenario covers a text search where the client asks for fields that are not returned from Lucene Stored Source. MongoT returns ids and scores, and mongod fetches the matched source documents from MongoDB storage before applying the client projection. That document fetch is inside mongod's execution plan. It is not the Java driver issuing a second query.

This document-fetch path does not require the Java driver to issue a second query. mongod can request enough MongoT hits, perform the document fetch inside mongod, and then send `killCursors` to MongoT. MongoT `getMore` is still a real command-stream path, but it belongs to cursor continuation workloads rather than being the defining feature of document fetch.

## Example client operation

```javascript
collection.aggregate([
  {
    "$search": {
      text: {
        path: "caption",
        query: "public domain observatory"
      }
    }
  },
  {
    "$project": {
      caption: 1,
      licenseName: 1,
      licenseUrl: 1,
      score: { "$meta": "searchScore" }
    }
  }
]);
```

## Walkthrough

1. The client sends one aggregate command to mongod through [AggregateOperationImpl.getCommand](https://github.com/mongodb/mongo-java-driver/blob/main/driver-core/src/main/com/mongodb/internal/operation/AggregateOperationImpl.java#L216).
2. mongod parses `$search` in [DocumentSourceSearch::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L128).
3. [DocumentSourceSearch::desugar](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L166) (the rewrite from public stage syntax into internal execution stages) creates the internal MongoT remote stage.
4. [search_helpers::promoteStoredSourceOrAddIdLookup](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L709) decides whether mongod can promote returned `storedSource` fields or must add [DocumentSourceInternalSearchIdLookUp](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_internal_search_id_lookup.cpp#L65).
5. [mongot_cursor::getRemoteCommandRequestForSearchQuery](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L49) builds the `search` command sent to MongoT, [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) targets the configured MongoT address, and [mongot_cursor::establishCursors](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L197) creates the MongoT cursor.
6. [ProjectFactory.build](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/ProjectFactory.java#L25) chooses how MongoT should shape returned fields.
7. If stored source is not being returned, [IdLookupFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/IdLookupFactory.java#L13) returns id-oriented documents. This lets mongod's internal id lookup fetch requested fields from the matched collection documents.
8. [SearchCommand.getBatch](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317) prepares the batch and cursor response.
9. mongod performs the id lookup and projection internally. The client-visible cursor is still a mongod cursor.
10. mongod can then call [KillCursorsCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/KillCursorsCommand.java#L33) to clean up the MongoT cursor after it has enough hits.
11. If a MongoT cursor does need continuation in another workload, [MongotTaskExecutorCursorGetMoreStrategy::createGetMoreRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor_getmore_strategy.cpp#L60) builds the MongoT `getMore` command and [GetMoreCommand.run](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/GetMoreCommand.java#L56) advances [MongotCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90).

## MongoDB server classes involved

- [DocumentSourceSearch::createFromBson](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L128) parses `$search`.
- [DocumentSourceSearch::desugar](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L166) rewrites `$search` from public stage syntax into the internal MongoT remote stage.
- [search_helpers::promoteStoredSourceOrAddIdLookup](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L709) chooses stored-source promotion or id lookup.
- [DocumentSourceInternalSearchIdLookUp](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_internal_search_id_lookup.cpp#L65) represents mongod's source-document lookup stage for MongoT hit ids.
- [mongot_cursor::establishCursorsForSearchStage](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L225) establishes the initial MongoT search cursor.
- [mongot_cursor::getRemoteCommandRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) points the initial search and conditional later cursor commands at the configured MongoT address.
- [MongotTaskExecutorCursorGetMoreStrategy::createGetMoreRequest](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor_getmore_strategy.cpp#L60) builds MongoT `getMore` requests when a MongoT cursor continues.
- [MongotTaskExecutorCursorGetMoreStrategy::_getNextDocsRequested](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor_getmore_strategy.cpp#L136) updates `docsRequested` using id-lookup success metrics when that continuation path is used.

## MongoT classes involved

- [SearchCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317) builds the first batch and cursor response.
- [ProjectFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/ProjectFactory.java#L25) chooses stored-source or id lookup behavior.
- [IdLookupFactory](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/IdLookupFactory.java#L13) builds id-based projection output.
- [MongotCursorManagerImpl](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L43) finds and owns cursor state.
- [KillCursorsCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/KillCursorsCommand.java#L33) handles the cleanup command in this scenario.
- [GetMoreCommand](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/server/command/search/GetMoreCommand.java#L56) handles conditional MongoT cursor continuation in other search workloads.
- [MongotCursor](https://github.com/mongodb/mongot/blob/main/src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90) obtains the next batch from the producer.

## Command messages

These examples use real message field names. Long arrays may be shortened with ellipses; no replacement fields are introduced. The client projection asks for `caption`, `licenseName`, and `licenseUrl`; the Stored Source includes `caption` but not `licenseName` or `licenseUrl`.

### mongod -> MongoT: search command

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
      "query": "a"
    }
  },
  "cursorOptions": {
    "batchSize": 10640
  },
  "$db": "clientDb"
}
```

### MongoT -> mongod: id-oriented search batch

```jsonc
{
  "cursor": {
    "id": 3500069593100182908,
    "ns": "clientDb.documents",
    "nextBatch": [
      {
        "_id": 72466,
        "$searchScore": 0.10923357307910919
      },
      {
        "_id": 190722,
        "$searchScore": 0.10923357307910919
      },
      {
        "_id": 10643,
        "$searchScore": 0.10923357307910919
      }
      // ... additional hit documents ...
    ]
  },
  "vars": {
    "SEARCH_META": {
      "count": {
        "lowerBound": 103203
      }
    }
  },
  "ok": 1
}
```

### mongod -> MongoT: cursor cleanup

```json
{
  "killCursors": "image",
  "cursors": [
    3500069593100182908
  ],
  "$db": "clientDb"
}
```

### MongoT -> mongod: cursor cleanup response

```json
{
  "ok": 1.0,
  "cursorsKilled": [
    3500069593100182908
  ],
  "cursorsNotFound": [],
  "cursorsAlive": [],
  "cursorsUnknown": []
}
```
