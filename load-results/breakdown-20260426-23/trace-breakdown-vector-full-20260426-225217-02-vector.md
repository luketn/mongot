# MongoT Trace Breakdown: Vector Search Only

Run window: 2026-04-26 22:57:31  to 2026-04-26 23:02:32 .

Command:

```sh
k6 run -e K6_VUS=see run env -e K6_DURATION=see run env -e SEARCH_TYPE=vector -e INCLUDE_LICENSE=false k6.js
```

Scenario:

Coco asks LM Studio for a query embedding, then sends /image/search with searchType=Vector. The aggregation contains a $search vectorSearch operator against captionEmbedding with exact cosine search and stored-source projection.

![Vector Search Only chart](./trace-breakdown-vector-full-20260426-225217-02-vector.png)

## Run Summary

| Metric | Value |
| --- | ---: |
| k6 requests | 56594 |
| k6 throughput | 188.589251 req/s |
| k6 failures | 0.00%   0 out of 56594 |
| HTTP median | 124.21 ms |
| HTTP p95 | 196.80645 ms |
| App-reported MongoDB median | 22.209688 ms |
| App-reported MongoDB p95 | 83.331783 ms |
| App Java non-Mongo median | 17.515479 ms |
| Jaeger traces sampled | 2,000 |
| Root trace operation | `mongodb.CommandService/atlasSearchCoco.image/search` |
| Median stream span | 9.979 ms |
| Median `mongot.search.command` | 2.797 ms |

## Representative Trace Attributes

| Attribute | Value |
| --- | --- |
| Trace id | `988a74ffc4dd80ebc40569a63ad663bf` |
| Operation id | `988a74ffc4dd80ebc40569a63ad663bf` |
| Search index | `vector_caption` |
| Query type | `operator` |
| Lucene query class | `com.xgen.mongot.index.lucene.query.custom.ExactVectorSearchQuery` |
| Lucene query | `ExactVectorSearchQuery:$type:knnVector/captionEmbedding[8.462216E-5, 0.08552499,...]` |
| Reader docs / segments | 118291 docs / 12 segments |
| Vector limit / type | 21 / EXACT |
| Result samples attached | 2 |

## Trace Segments

| Segment | Median | Share | Span(s) | Code | Description |
| --- | ---: | ---: | --- | --- | --- |
| query context | 28 us | 1.00% of command | `mongot.search.prepare_query_context` | [SearchCommand.run](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129)<br>[OptimizationFlagsDefinition.toQueryOptimizationFlags](../../src/main/java/com/xgen/mongot/server/command/search/definition/request/OptimizationFlagsDefinition.java#L31)<br>[DynamicFeatureFlagRegistry.evaluateClusterInvariant](../../src/main/java/com/xgen/mongot/featureflag/dynamic/DynamicFeatureFlagRegistry.java#L128) | Prepares per-query execution flags and feature-flag state before BSON parsing. This is in-memory control-plane work. |
| parse BSON | 507.5 us | 18.14% of command | `mongot.search.parse_query_bson`, `mongot.search.parse_query`, `Query.fromBson` | [SearchCommand.run](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129)<br>[SearchQuery.fromBson](../../src/main/java/com/xgen/mongot/index/query/SearchQuery.java#L171)<br>[Query.fromBson span](../../src/main/java/com/xgen/mongot/index/query/SearchQuery.java#L176) | Converts the incoming BSON $search command into MongoT query model objects. This parses text, vectorSearch, compound, facet, and filter clauses before any Lucene execution. |
| index lookup | 4 us | 0.14% of command | `mongot.search.lookup_index_catalog`, `mongot.search.resolve_index` | [SearchCommand.getIndexFromCatalog](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L477) | Looks up the named search index in MongoT's in-memory catalog and records whether the index and partitions are available. |
| response mode | 1 us | 0.04% of command | `mongot.search.prepare_response_mode` | [SearchCommand.determinePopulateCursor](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L539)<br>[SearchCommand.addMetadataIfExplain](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L522) | Decides whether to populate cursor results and whether explain metadata is required. |
| cursor setup | 3 us | 0.11% of command | `mongot.cursor.select_index_manager`, `mongot.cursor.choose_batch_size`, `mongot.cursor.instantiate_cursor`, `mongot.cursor.register_active_cursor`, `mongot.cursor.register_index_mapping` | [MongotCursorManagerImpl.newCursor](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L130)<br>[CursorFactory.createCursor](../../src/main/java/com/xgen/mongot/cursor/CursorFactory.java#L57)<br>[IndexCursorManagerImpl.createCursor](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L83) | Creates and registers cursor state around the index reader and batch producer. |
| cursor orchestration | 50 us | 1.79% of command | `mongot.search.create_and_register_cursor` | [MongotCursorManagerImpl.newCursor](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L130)<br>[CursorFactory.createCursor](../../src/main/java/com/xgen/mongot/cursor/CursorFactory.java#L57)<br>[IndexCursorManagerImpl.createCursor](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L83) | Residual cursor creation work: locks, availability checks, manager handoff, and cursor lifecycle wiring. |
| Lucene open searcher | 1 us | 0.04% of command | `mongot.lucene.open_index_searcher` | [LuceneSearchIndexReader.createSearcherReference](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L1401)<br>[LuceneIndexSearcherReference.create](../../src/main/java/com/xgen/mongot/index/lucene/LuceneIndexSearcherReference.java#L102) | Acquires a Lucene [`IndexSearcher`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html) view for the current searcher generation and records reader doc/segment counts. This is a Lucene searcher-manager acquire path, not query execution. |
| build query | 20 us | 0.72% of command | `mongot.lucene.build_query`, `mongot.lucene.create_search_query` | [LuceneSearchQueryFactoryDistributor.createQuery](../../src/main/java/com/xgen/mongot/index/lucene/query/LuceneSearchQueryFactoryDistributor.java#L229)<br>[TextQueryFactory.createQuery](../../src/main/java/com/xgen/mongot/index/lucene/query/TextQueryFactory.java#L125)<br>[VectorSearchQueryFactory.query](../../src/main/java/com/xgen/mongot/index/lucene/query/VectorSearchQueryFactory.java#L304) | Converts MongoT query objects into Lucene [`Query`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/Query.html) objects. The span stores the Lucene query representation, class, size, hash, and truncation flag. This constructs the Lucene query tree but does not execute it. |
| Lucene vector candidates | 1.903 ms | 68.06% of command | `mongot.lucene.vector_collect_candidates` | [LuceneVectorIndexReader.queryResults](../../src/main/java/com/xgen/mongot/index/lucene/LuceneVectorIndexReader.java#L180)<br>[LuceneVectorSearchManager.initialSearch](../../src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L50)<br>[VectorSearchQueryFactory.query](../../src/main/java/com/xgen/mongot/index/lucene/query/VectorSearchQueryFactory.java#L304) | Executes the Lucene vector candidate collection path over `captionEmbedding`. The generated query is a Lucene [`Query`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/Query.html), backed by MongoT vector query classes or Lucene KNN query behavior, and the search returns [`TopDocs`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/TopDocs.html) candidates before BSON materialization. |
| Lucene sort/highlight setup | 1 us | 0.04% of command | `mongot.lucene.prepare_highlights`, `mongot.lucene.prepare_highlighter`, `mongot.lucene.prepare_score_details`, `mongot.lucene.prepare_pagination_sort_context`, `mongot.lucene.build_sort`, `mongot.lucene.create_sort` | [LuceneHighlighterContext.getHighlighterIfPresent](../../src/main/java/com/xgen/mongot/index/lucene/LuceneHighlighterContext.java#L35)<br>[LuceneScoreDetailsManager.getScoreDetailsManagerIfPresent](../../src/main/java/com/xgen/mongot/index/lucene/LuceneScoreDetailsManager.java#L83)<br>[LuceneSearchQueryFactoryDistributor.createSort](../../src/main/java/com/xgen/mongot/index/lucene/query/LuceneSearchQueryFactoryDistributor.java#L336) | Builds optional Lucene [`Sort`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/Sort.html), pagination, highlighting, and score-detail helpers. Highlighting can use Lucene [`UnifiedHighlighter`](https://lucene.apache.org/core/9_11_1/highlighter/org/apache/lucene/search/uhighlight/UnifiedHighlighter.html), and score details can call [`IndexSearcher.explain(Query, int)`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html#explain(org.apache.lucene.search.Query,int)). |
| reader orchestration | 49 us | 1.75% of command | `mongot.lucene.prepare_search_reader_query`, `mongot.lucene.search_index_reader.query` | [LuceneSearchIndexReader.query](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L250)<br>[LuceneSearchIndexReader.collectorQuery](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L672) | Residual work in the reader query path: stored-source checks, query branch dispatch, shared lock acquisition, and reader bookkeeping. |
| Lucene advance batch | 22 us | 0.79% of command | `mongot.cursor.advance_batch_producer` | [MongotCursor.getExplainDisabledNextBatch](../../src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90)<br>[LuceneSearchBatchProducer.execute](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170)<br>[AbstractLuceneSearchManager.getMoreTopDocs](../../src/main/java/com/xgen/mongot/index/lucene/AbstractLuceneSearchManager.java#L33) | Advances the batch producer. Later getMore calls may invoke Lucene [`IndexSearcher.searchAfter`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html#searchAfter(org.apache.lucene.search.ScoreDoc,org.apache.lucene.search.Query,int)) to continue from the previous [`ScoreDoc`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/ScoreDoc.html). |
| materialize BSON | 102 us | 3.65% of command | `mongot.lucene.materialize_bson_documents`, `mongot.lucene.materialize_results` | [LuceneSearchBatchProducer.getSearchResultsFromIter](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L299)<br>[ProjectStage.project](../../src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/ProjectStage.java#L35)<br>[MetaIdRetriever.getRootMetaId](../../src/main/java/com/xgen/mongot/index/lucene/query/util/MetaIdRetriever.java#L34) | Converts Lucene hits into BSON response documents, including stored-source or document-field materialization and sampled result payload attributes. Stored-source paths use Lucene [`IndexReader.storedFields()`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/IndexReader.html#storedFields()) and [`StoredFields.document`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/StoredFields.html#document(int)). |
| batch orchestration | 29 us | 1.04% of command | `mongot.search.load_first_cursor_batch` | [MongotCursorManagerImpl.getNextBatch](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L226)<br>[IndexCursorManagerImpl.getNextBatch](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L150)<br>[MongotCursor.getNextBatch](../../src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L67) | Residual first-batch loading work around producer advancement, materialization, cursor state, and exhaustion checks. |
| response doc | 5 us | 0.18% of command | `mongot.search.prepare_response_document` | [SearchCommand.getBatch](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317)<br>[MongotCursorBatch.toBson](../../src/main/java/com/xgen/mongot/cursor/serialization/MongotCursorBatch.java#L100) | Builds the command response wrapper, cursor result, metadata variables, and batch bookkeeping. |
| encode BSON | 1 us | 0.04% of command | `mongot.search.encode_response_bson`, `mongot.search.serialize_batch` | [SearchCommand.getBatch](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317)<br>[MongotCursorBatch.toBson](../../src/main/java/com/xgen/mongot/cursor/serialization/MongotCursorBatch.java#L100) | Serializes the response document into the BSON/protobuf response payload returned over the command stream. |
| command orchestration | 0 us | 0.00% of command | `mongot.search.command` | [SearchCommand.run](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129)<br>[SearchCommand.getBatch](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317)<br>[CursorGuard](../../src/main/java/com/xgen/mongot/server/command/search/CursorGuard.java) | Residual command work: span attributes, metrics, cursor guard control flow, branch handling, and normal return/error flow. |
| stream lifecycle | 4.825 ms | 48.35% overall | `mongodb.CommandService/atlasSearchCoco.image/search` | [ServerCallHandler.onNext](../../src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L82)<br>[ServerCallHandler.handleMessage](../../src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126)<br>[CommandManager](../../src/main/java/com/xgen/mongot/server/grpc/CommandManager.java#L34) | Full command stream time outside the initial command span. This includes gRPC lifecycle, response observer handling, client consumption, half-close/cleanup, and later cursor batches in the same stream. |

## Notes

The root stream span is the most representative end-to-end MongoT view because it includes gRPC stream lifecycle, the initial command span, and later cursor/getMore work in the same stream. The command-phase percentages show only the initial `mongot.search.command` span so Lucene/query/materialization work remains visible.
