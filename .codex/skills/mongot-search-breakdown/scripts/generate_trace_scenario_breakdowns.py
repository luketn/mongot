#!/usr/bin/env python3
import argparse
import json
import re
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont


ROOT_OPERATION = "mongodb.CommandService/atlasSearchCoco.image/search"


@dataclass(frozen=True)
class Scenario:
    slug: str
    title: str
    kind: str
    script: str
    env: dict[str, str]
    description: str
    operation: str = ROOT_OPERATION


@dataclass(frozen=True)
class Segment:
    key: str
    label: str
    spans: tuple[str, ...]
    code: str
    description: str
    computed: bool = False


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(
        description="Run Atlas Search Coco k6 scenarios and generate MongoT trace breakdown reports."
    )
    parser.add_argument("--repo-root", type=Path, default=root)
    parser.add_argument("--coco-dir", type=Path, default=Path("/Users/luketn/code/personal/atlas-search-coco"))
    parser.add_argument("--output-dir", type=Path, default=root / "load-results/breakdown")
    parser.add_argument("--jaeger-url", default="http://localhost:16686")
    parser.add_argument("--base-url", default="http://localhost:8222")
    parser.add_argument("--vus", default="25")
    parser.add_argument("--duration", default="5m")
    parser.add_argument(
        "--mutation-vus",
        default="1",
        help="Mutation defaults to one VU because Atlas Search Coco /image/add allocates ids with nextImageId(), which races under concurrent inserts.",
    )
    parser.add_argument("--mutation-duration", default="")
    parser.add_argument("--trace-limit", type=int, default=2000)
    parser.add_argument(
        "--scenarios",
        default="text,vector,both,text-license,mutations",
        help="Comma-separated scenario ids: text, vector, both, text-license, mutations",
    )
    parser.add_argument("--skip-k6", action="store_true")
    parser.add_argument("--run-prefix", default=datetime.now().strftime("%Y%m%d-%H%M%S"))
    return parser.parse_args()


def scenarios_for(args: argparse.Namespace) -> dict[str, Scenario]:
    return {
        "text": Scenario(
            slug="text",
            title="Text Search Only",
            kind="search",
            script="k6.js",
            env={"SEARCH_TYPE": "text", "INCLUDE_LICENSE": "false"},
            description=(
                "Coco sends a text-only /image/search request. The Java app builds an aggregation "
                "with a $search facet collector, a compound text clause over caption, optional facet "
                "filters, stored-source response projection, and a final response facet."
            ),
        ),
        "vector": Scenario(
            slug="vector",
            title="Vector Search Only",
            kind="search",
            script="k6.js",
            env={"SEARCH_TYPE": "vector", "INCLUDE_LICENSE": "false"},
            description=(
                "Coco asks LM Studio for a query embedding, then sends /image/search with searchType=Vector. "
                "The aggregation contains a $search vectorSearch operator against captionEmbedding with exact "
                "cosine search and stored-source projection."
            ),
        ),
        "both": Scenario(
            slug="both",
            title="Both Vector And Text",
            kind="search",
            script="k6.js",
            env={"SEARCH_TYPE": "both", "INCLUDE_LICENSE": "false"},
            description=(
                "Coco sends both searchType=Text and searchType=Vector. The Java app builds a $rankFusion "
                "aggregation with one text $search pipeline and one vector $search pipeline, then sorts and "
                "projects the fused results."
            ),
        ),
        "text-license": Scenario(
            slug="text-license",
            title="Text Search With License Fields",
            kind="search",
            script="k6.js",
            env={"SEARCH_TYPE": "text", "INCLUDE_LICENSE": "true"},
            description=(
                "Coco sends text search with includeLicense=true. That disables returnStoredSource for the "
                "application projection path, so result materialization needs the full document fields required "
                "for licenseName and licenseUrl rather than only the stored-source subset."
            ),
        ),
        "mutations": Scenario(
            slug="mutations",
            title="Random Synthetic Deletes And Inserts",
            kind="mutation",
            script="k6-mutations.js",
            env={"K6_MUTATION_MODE": "paired"},
            description=(
                "A separate k6 workload drives /image/add and /image/delete through the Coco API using "
                "synthetic images. It inserts only generated documents and deletes those generated ids, so the "
                "COCO dataset is not intentionally modified."
            ),
        ),
    }


SEARCH_SEGMENTS: tuple[Segment, ...] = (
    Segment(
        "query_context",
        "query context",
        ("mongot.search.prepare_query_context",),
        "`SearchCommand.run`, `OptimizationFlagsDefinition.toQueryOptimizationFlags`, `DynamicFeatureFlagRegistry.evaluateClusterInvariant`",
        "Prepares per-query execution flags and feature-flag state before BSON parsing. This is in-memory control-plane work.",
    ),
    Segment(
        "parse_bson",
        "parse BSON",
        ("mongot.search.parse_query_bson", "mongot.search.parse_query", "Query.fromBson"),
        "`SearchCommand.run`, `SearchQuery.fromBson`, query definition classes under `com.xgen.mongot.index.query`",
        "Converts the incoming BSON $search command into MongoT query model objects. This parses text, vectorSearch, compound, facet, and filter clauses before any Lucene execution.",
    ),
    Segment(
        "index_lookup",
        "index lookup",
        ("mongot.search.lookup_index_catalog", "mongot.search.resolve_index"),
        "`SearchCommand.getIndexFromCatalog`, initialized index catalog",
        "Looks up the named search index in MongoT's in-memory catalog and records whether the index and partitions are available.",
    ),
    Segment(
        "cursor_options",
        "cursor options",
        ("mongot.search.parse_cursor_options",),
        "`CursorOptionsDefinition.toQueryCursorOptions`",
        "Parses optional cursor settings from the command. The Coco k6 requests usually take the fast default path.",
    ),
    Segment(
        "validate",
        "validate",
        ("mongot.search.validate_query_options", "mongot.search.validate_query"),
        "`SearchCommand.validateQueryAndCursorOptions`",
        "Validates search command options against query shape and cursor semantics.",
    ),
    Segment(
        "response_mode",
        "response mode",
        ("mongot.search.prepare_response_mode",),
        "`SearchCommand.determinePopulateCursor`, `SearchCommand.addMetadataIfExplain`",
        "Decides whether to populate cursor results and whether explain metadata is required.",
    ),
    Segment(
        "cursor_setup",
        "cursor setup",
        (
            "mongot.cursor.select_index_manager",
            "mongot.cursor.choose_batch_size",
            "mongot.cursor.instantiate_cursor",
            "mongot.cursor.register_active_cursor",
            "mongot.cursor.register_index_mapping",
        ),
        "`MongotCursorManagerImpl.newCursor`, `CursorFactory.createCursor`, `IndexCursorManagerImpl.createCursor`",
        "Creates and registers cursor state around the index reader and batch producer.",
    ),
    Segment(
        "cursor_orchestration",
        "cursor orchestration",
        ("mongot.search.create_and_register_cursor",),
        "`mongot.search.create_and_register_cursor` residual after cursor setup and reader query spans",
        "Residual cursor creation work: locks, availability checks, manager handoff, and cursor lifecycle wiring.",
        computed=True,
    ),
    Segment(
        "Lucene_open_searcher",
        "Lucene open searcher",
        ("mongot.lucene.open_index_searcher",),
        "`LuceneSearchIndexReader.query`, SearcherManager acquire path",
        "Acquires a Lucene [`IndexSearcher`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html) view for the current searcher generation and records reader doc/segment counts. This is a Lucene searcher-manager acquire path, not query execution.",
    ),
    Segment(
        "build_query",
        "build query",
        ("mongot.lucene.build_query", "mongot.lucene.create_search_query"),
        "`LuceneSearchQueryFactoryDistributor.createQuery`, text/vector query factories",
        "Converts MongoT query objects into Lucene [`Query`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/Query.html) objects. The span stores the Lucene query representation, class, size, hash, and truncation flag. This constructs the Lucene query tree but does not execute it.",
    ),
    Segment(
        "Lucene_collect_hits",
        "Lucene collect hits",
        ("mongot.lucene.collect_initial_top_docs", "mongot.lucene.initial_top_docs"),
        "`MeteredLuceneSearchManager.initialSearch`, `IndexSearcher.search(Query, CollectorManager)`",
        "Executes the initial Lucene text/facet search through [`IndexSearcher.search(Query, CollectorManager)`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html#search(org.apache.lucene.search.Query,org.apache.lucene.search.CollectorManager)). Lucene walks index segments, scores matches, and returns [`TopDocs`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/TopDocs.html) for the first page.",
    ),
    Segment(
        "Lucene_vector_candidates",
        "Lucene vector candidates",
        ("mongot.lucene.vector_collect_candidates",),
        "`LuceneVectorIndexReader.queryResults`, Lucene vector candidate collection",
        "Executes the Lucene vector candidate collection path over `captionEmbedding`. The generated query is a Lucene [`Query`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/Query.html), backed by MongoT vector query classes or Lucene KNN query behavior, and the search returns [`TopDocs`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/TopDocs.html) candidates before BSON materialization.",
    ),
    Segment(
        "Lucene_sort_highlight",
        "Lucene sort/highlight setup",
        (
            "mongot.lucene.prepare_highlights",
            "mongot.lucene.prepare_highlighter",
            "mongot.lucene.prepare_score_details",
            "mongot.lucene.prepare_pagination_sort_context",
            "mongot.lucene.build_sort",
            "mongot.lucene.create_sort",
        ),
        "`LuceneSearchIndexReader` sort, pagination, highlighter, and score-detail preparation",
        "Builds optional Lucene [`Sort`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/Sort.html), pagination, highlighting, and score-detail helpers. Highlighting can use Lucene [`UnifiedHighlighter`](https://lucene.apache.org/core/9_11_1/highlighter/org/apache/lucene/search/uhighlight/UnifiedHighlighter.html), and score details can call [`IndexSearcher.explain(Query, int)`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html#explain(org.apache.lucene.search.Query,int)).",
    ),
    Segment(
        "reader_orchestration",
        "reader orchestration",
        ("mongot.lucene.prepare_search_reader_query", "mongot.lucene.search_index_reader.query"),
        "`LuceneSearchIndexReader.query` residual around named Lucene child spans",
        "Residual work in the reader query path: stored-source checks, query branch dispatch, shared lock acquisition, and reader bookkeeping.",
        computed=True,
    ),
    Segment(
        "Lucene_advance_batch",
        "Lucene advance batch",
        ("mongot.cursor.advance_batch_producer",),
        "`MongotCursor.getNextBatch`, `LuceneSearchBatchProducer.execute`",
        "Advances the batch producer. Later getMore calls may invoke Lucene [`IndexSearcher.searchAfter`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html#searchAfter(org.apache.lucene.search.ScoreDoc,org.apache.lucene.search.Query,int)) to continue from the previous [`ScoreDoc`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/ScoreDoc.html).",
    ),
    Segment(
        "materialize_bson",
        "materialize BSON",
        ("mongot.lucene.materialize_bson_documents", "mongot.lucene.materialize_results"),
        "`LuceneSearchBatchProducer.getSearchResultsFromIter`, `ProjectStage.project`, stored fields access",
        "Converts Lucene hits into BSON response documents, including stored-source or document-field materialization and sampled result payload attributes. Stored-source paths use Lucene [`IndexReader.storedFields()`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/IndexReader.html#storedFields()) and [`StoredFields.document`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/StoredFields.html#document(int)).",
    ),
    Segment(
        "batch_orchestration",
        "batch orchestration",
        ("mongot.search.load_first_cursor_batch",),
        "`MongotCursorManagerImpl.getNextBatch`, `IndexCursorManagerImpl.getNextBatch` residual",
        "Residual first-batch loading work around producer advancement, materialization, cursor state, and exhaustion checks.",
        computed=True,
    ),
    Segment(
        "response_doc",
        "response doc",
        ("mongot.search.prepare_response_document",),
        "`SearchCommand.getBatch`, `MongotCursorBatch`",
        "Builds the command response wrapper, cursor result, metadata variables, and batch bookkeeping.",
    ),
    Segment(
        "encode_bson",
        "encode BSON",
        ("mongot.search.encode_response_bson", "mongot.search.serialize_batch"),
        "`SearchCommand.getBatch`, gRPC/protobuf BSON serialization path",
        "Serializes the response document into the BSON/protobuf response payload returned over the command stream.",
    ),
    Segment(
        "command_orchestration",
        "command orchestration",
        ("mongot.search.command",),
        "`mongot.search.command` residual after named command-phase spans",
        "Residual command work: span attributes, metrics, cursor guard control flow, branch handling, and normal return/error flow.",
        computed=True,
    ),
    Segment(
        "stream_lifecycle",
        "stream lifecycle",
        (ROOT_OPERATION,),
        "`ServerCallHandler`, `CommandManager`, gRPC stream callbacks and client consumption",
        "Full command stream time outside the initial command span. This includes gRPC lifecycle, response observer handling, client consumption, half-close/cleanup, and later cursor batches in the same stream.",
        computed=True,
    ),
)


MUTATION_SEGMENTS: tuple[Segment, ...] = (
    Segment(
        "change_stream_batch",
        "change stream batch",
        ("mongot.indexing.change_stream_batch",),
        "`DecodingExecutorChangeStreamIndexManager` change stream callback",
        "Receives a MongoDB change stream batch for an index generation and records resume/lifecycle metadata.",
    ),
    Segment(
        "decode_batch",
        "decode batch",
        ("mongot.indexing.decode_batch",),
        "`DecodingWorkScheduler`, `SchedulerQueue`",
        "Moves raw change stream work into the decoding scheduler and preserves tracing context for downstream indexing work.",
    ),
    Segment(
        "decode_events",
        "decode change events",
        ("mongot.indexing.decode_change_stream_events",),
        "`DecodingExecutorChangeStreamIndexManager.decodeBatch`",
        "Decodes MongoDB change stream events into MongoT document events and records witnessed/applicable counts.",
    ),
    Segment(
        "indexing_batch",
        "indexing batch",
        ("mongot.indexing.batch",),
        "`DefaultIndexingWorkScheduler`, `IndexingWorkScheduler`",
        "Runs an indexing scheduler batch after decoding. This is the bridge from MongoDB change events into index writer mutations.",
    ),
    Segment(
        "document_insert",
        "document event INSERT",
        ("mongot.indexing.document_event",),
        "`SingleLuceneIndexWriter.updateIndex` INSERT path",
        "Processes one insert/update document event before converting it into Lucene index writer operations.",
    ),
    Segment(
        "document_delete",
        "document event DELETE",
        ("mongot.indexing.document_event",),
        "`SingleLuceneIndexWriter.updateIndex` DELETE path",
        "Processes one delete document event before calling the Lucene delete path.",
    ),
    Segment(
        "writer_update_index",
        "writer update index",
        ("mongot.lucene.index_writer.update_index",),
        "`SingleLuceneIndexWriter.updateIndex`",
        "Top-level per-document Lucene writer mutation wrapper. It routes insert/update/delete events to document block construction, updateDocuments, or deleteDocuments.",
    ),
    Segment(
        "build_document_block",
        "build document block",
        ("mongot.lucene.index_writer.build_document_block",),
        "`SingleLuceneIndexWriter.buildDocumentBlock`, ingestion/document builders",
        "Converts MongoDB BSON fields into the Lucene document block for indexed and stored fields.",
    ),
    Segment(
        "Lucene_update_documents",
        "Lucene update documents",
        ("mongot.lucene.index_writer.update_documents",),
        "`SingleLuceneIndexWriter`, Lucene `IndexWriter.updateDocuments`",
        "Invokes Lucene [`IndexWriter.updateDocuments`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/IndexWriter.html#updateDocuments(org.apache.lucene.index.Term,java.lang.Iterable)) to atomically replace the document block for the synthetic inserted document.",
    ),
    Segment(
        "Lucene_delete_documents",
        "Lucene delete documents",
        ("mongot.lucene.index_writer.delete_documents",),
        "`SingleLuceneIndexWriter`, Lucene `IndexWriter.deleteDocuments`",
        "Invokes Lucene [`IndexWriter.deleteDocuments`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/IndexWriter.html#deleteDocuments(org.apache.lucene.index.Term...)) to delete documents matching the MongoDB document id term.",
    ),
    Segment(
        "Lucene_commit",
        "Lucene commit",
        ("mongot.lucene.index_writer.commit",),
        "`SingleLuceneIndexWriter.commit`, Lucene `IndexWriter.commit`",
        "Commits writer changes and user data through Lucene [`IndexWriter.commit`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/IndexWriter.html#commit()). This can happen on a periodic schedule rather than in the exact same trace as the API request.",
    ),
    Segment(
        "refresh_searcher",
        "refresh searcher",
        ("mongot.lucene.maybe_refresh_searcher_manager",),
        "`PeriodicLuceneIndexRefresher`, Lucene SearcherManager refresh",
        "Refreshes the Lucene searcher manager so newly committed index changes become visible to searchers. This uses Lucene [`SearcherManager`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/SearcherManager.html) refresh behavior.",
    ),
)


CODE_REFERENCES: dict[str, str] = {
    "query_context": "[SearchCommand.run](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129)<br>[OptimizationFlagsDefinition.toQueryOptimizationFlags](../../src/main/java/com/xgen/mongot/server/command/search/definition/request/OptimizationFlagsDefinition.java#L31)<br>[DynamicFeatureFlagRegistry.evaluateClusterInvariant](../../src/main/java/com/xgen/mongot/featureflag/dynamic/DynamicFeatureFlagRegistry.java#L128)",
    "parse_bson": "[SearchCommand.run](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129)<br>[SearchQuery.fromBson](../../src/main/java/com/xgen/mongot/index/query/SearchQuery.java#L171)<br>[Query.fromBson span](../../src/main/java/com/xgen/mongot/index/query/SearchQuery.java#L176)",
    "index_lookup": "[SearchCommand.getIndexFromCatalog](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L477)",
    "cursor_options": "[CursorOptionsDefinition.toQueryCursorOptions](../../src/main/java/com/xgen/mongot/server/command/search/definition/request/CursorOptionsDefinition.java#L65)",
    "validate": "[SearchCommand.validateQueryAndCursorOptions](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L301)",
    "response_mode": "[SearchCommand.determinePopulateCursor](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L539)<br>[SearchCommand.addMetadataIfExplain](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L522)",
    "cursor_setup": "[MongotCursorManagerImpl.newCursor](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L130)<br>[CursorFactory.createCursor](../../src/main/java/com/xgen/mongot/cursor/CursorFactory.java#L57)<br>[IndexCursorManagerImpl.createCursor](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L83)",
    "cursor_orchestration": "[MongotCursorManagerImpl.newCursor](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L130)<br>[CursorFactory.createCursor](../../src/main/java/com/xgen/mongot/cursor/CursorFactory.java#L57)<br>[IndexCursorManagerImpl.createCursor](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L83)",
    "Lucene_open_searcher": "[LuceneSearchIndexReader.createSearcherReference](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L1401)<br>[LuceneIndexSearcherReference.create](../../src/main/java/com/xgen/mongot/index/lucene/LuceneIndexSearcherReference.java#L102)",
    "build_query": "[LuceneSearchQueryFactoryDistributor.createQuery](../../src/main/java/com/xgen/mongot/index/lucene/query/LuceneSearchQueryFactoryDistributor.java#L229)<br>[TextQueryFactory.createQuery](../../src/main/java/com/xgen/mongot/index/lucene/query/TextQueryFactory.java#L125)<br>[VectorSearchQueryFactory.query](../../src/main/java/com/xgen/mongot/index/lucene/query/VectorSearchQueryFactory.java#L304)",
    "Lucene_collect_hits": "[MeteredLuceneSearchManager.initialSearch](../../src/main/java/com/xgen/mongot/index/lucene/MeteredLuceneSearchManager.java#L30)<br>[LuceneOperatorSearchManager.initialSearch](../../src/main/java/com/xgen/mongot/index/lucene/LuceneOperatorSearchManager.java#L34)<br>[AbstractLuceneSearchManager.createCollectorManager](../../src/main/java/com/xgen/mongot/index/lucene/AbstractLuceneSearchManager.java#L59)",
    "Lucene_vector_candidates": "[LuceneVectorIndexReader.queryResults](../../src/main/java/com/xgen/mongot/index/lucene/LuceneVectorIndexReader.java#L180)<br>[LuceneVectorSearchManager.initialSearch](../../src/main/java/com/xgen/mongot/index/lucene/LuceneVectorSearchManager.java#L50)<br>[VectorSearchQueryFactory.query](../../src/main/java/com/xgen/mongot/index/lucene/query/VectorSearchQueryFactory.java#L304)",
    "Lucene_sort_highlight": "[LuceneHighlighterContext.getHighlighterIfPresent](../../src/main/java/com/xgen/mongot/index/lucene/LuceneHighlighterContext.java#L35)<br>[LuceneScoreDetailsManager.getScoreDetailsManagerIfPresent](../../src/main/java/com/xgen/mongot/index/lucene/LuceneScoreDetailsManager.java#L83)<br>[LuceneSearchQueryFactoryDistributor.createSort](../../src/main/java/com/xgen/mongot/index/lucene/query/LuceneSearchQueryFactoryDistributor.java#L336)",
    "reader_orchestration": "[LuceneSearchIndexReader.query](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L250)<br>[LuceneSearchIndexReader.collectorQuery](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L672)",
    "Lucene_advance_batch": "[MongotCursor.getExplainDisabledNextBatch](../../src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90)<br>[LuceneSearchBatchProducer.execute](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L170)<br>[AbstractLuceneSearchManager.getMoreTopDocs](../../src/main/java/com/xgen/mongot/index/lucene/AbstractLuceneSearchManager.java#L33)",
    "materialize_bson": "[LuceneSearchBatchProducer.getSearchResultsFromIter](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L299)<br>[ProjectStage.project](../../src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/ProjectStage.java#L35)<br>[MetaIdRetriever.getRootMetaId](../../src/main/java/com/xgen/mongot/index/lucene/query/util/MetaIdRetriever.java#L34)",
    "batch_orchestration": "[MongotCursorManagerImpl.getNextBatch](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L226)<br>[IndexCursorManagerImpl.getNextBatch](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L150)<br>[MongotCursor.getNextBatch](../../src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L67)",
    "response_doc": "[SearchCommand.getBatch](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317)<br>[MongotCursorBatch.toBson](../../src/main/java/com/xgen/mongot/cursor/serialization/MongotCursorBatch.java#L100)",
    "encode_bson": "[SearchCommand.getBatch](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317)<br>[MongotCursorBatch.toBson](../../src/main/java/com/xgen/mongot/cursor/serialization/MongotCursorBatch.java#L100)",
    "command_orchestration": "[SearchCommand.run](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129)<br>[SearchCommand.getBatch](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317)<br>[CursorGuard](../../src/main/java/com/xgen/mongot/server/command/search/CursorGuard.java)",
    "stream_lifecycle": "[ServerCallHandler.onNext](../../src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L82)<br>[ServerCallHandler.handleMessage](../../src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L126)<br>[CommandManager](../../src/main/java/com/xgen/mongot/server/grpc/CommandManager.java#L34)",
    "change_stream_batch": "[DecodingExecutorChangeStreamIndexManager](../../src/main/java/com/xgen/mongot/replication/mongodb/steadystate/changestream/DecodingExecutorChangeStreamIndexManager.java#L151)",
    "decode_batch": "[DecodingWorkScheduler.run](../../src/main/java/com/xgen/mongot/replication/mongodb/common/DecodingWorkScheduler.java#L146)",
    "decode_events": "[DecodingExecutorChangeStreamIndexManager.decodeBatch](../../src/main/java/com/xgen/mongot/replication/mongodb/steadystate/changestream/DecodingExecutorChangeStreamIndexManager.java#L169)",
    "indexing_batch": "[IndexingWorkScheduler.run](../../src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java#L159)<br>[DefaultIndexingWorkScheduler.getBatchTasksFuture](../../src/main/java/com/xgen/mongot/replication/mongodb/common/DefaultIndexingWorkScheduler.java#L34)",
    "document_insert": "[IndexingWorkScheduler.DocumentEventTask.run](../../src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java#L374)<br>[SingleLuceneIndexWriter.updateIndex](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332)",
    "document_delete": "[IndexingWorkScheduler.DocumentEventTask.run](../../src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java#L374)<br>[SingleLuceneIndexWriter.updateIndex](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332)",
    "writer_update_index": "[SingleLuceneIndexWriter.updateIndex](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332)",
    "build_document_block": "[SingleLuceneIndexWriter.updateIndex](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332)<br>[document block build span](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L597)",
    "Lucene_update_documents": "[SingleLuceneIndexWriter.updateIndex](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332)<br>[IndexWriter.updateDocuments call](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L643)",
    "Lucene_delete_documents": "[SingleLuceneIndexWriter.updateIndex](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L332)<br>[IndexWriter.deleteDocuments call](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L567)",
    "Lucene_commit": "[SingleLuceneIndexWriter.commit](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L349)<br>[IndexWriter.commit call](../../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java#L363)",
    "refresh_searcher": "[PeriodicLuceneIndexRefresher.run](../../src/main/java/com/xgen/mongot/index/lucene/PeriodicLuceneIndexRefresher.java#L93)",
}


def segment_code(segment: Segment) -> str:
    return CODE_REFERENCES.get(segment.key, segment.code)


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    names = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Helvetica.ttf",
    ]
    for name in names:
        try:
            return ImageFont.truetype(name, size=size)
        except OSError:
            pass
    return ImageFont.load_default()


def format_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S %Z")


def median(values: list[float]) -> float:
    clean = [value for value in values if value is not None]
    return statistics.median(clean) if clean else 0.0


def fmt_us(micros: float) -> str:
    if micros >= 1000:
        return f"{micros / 1000:.3f} ms".rstrip("0").rstrip(".")
    if float(micros).is_integer():
        return f"{int(micros)} us"
    return f"{micros:.1f} us"


def pct(value: float, denominator: float) -> str:
    if not denominator:
        return "0.00%"
    return f"{(value / denominator) * 100:.2f}%"


def line_value(text: str, key: str) -> str:
    match = re.search(r"^\s*" + re.escape(key) + r"\.*:\s*(.*)$", text, re.M)
    return match.group(1).strip() if match else ""


def metric_number(line: str, key: str) -> str:
    match = re.search(re.escape(key) + r"=([0-9.]+)", line)
    return match.group(1) if match else ""


def run_k6(args: argparse.Namespace, scenario: Scenario, run_id: str) -> tuple[int, int, Path, Path]:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.output_dir / f"k6-{scenario.slug}-{run_id}.log"
    env_path = args.output_dir / f"run-{scenario.slug}-{run_id}.env"

    vus = args.mutation_vus if scenario.kind == "mutation" and args.mutation_vus else args.vus
    duration = args.mutation_duration if scenario.kind == "mutation" and args.mutation_duration else args.duration
    command = ["k6", "run", "-e", f"K6_VUS={vus}", "-e", f"K6_DURATION={duration}", "-e", f"BASE_URL={args.base_url}"]
    for key, value in scenario.env.items():
        command.extend(["-e", f"{key}={value}"])
    if scenario.kind == "mutation":
        command.extend(["-e", f"K6_MUTATION_PREFIX={scenario.slug}-{run_id}"])
    command.append(scenario.script)

    start_epoch = int(time.time())
    with log_path.open("w") as log_file:
        result = subprocess.run(command, cwd=args.coco_dir, stdout=log_file, stderr=subprocess.STDOUT, check=False)
    end_epoch = int(time.time())
    env_path.write_text(
        "\n".join(
            [
                f"SCENARIO={scenario.slug}",
                f"TITLE={scenario.title}",
                f"RUN_ID={run_id}",
                f"START_EPOCH={start_epoch}",
                f"START_ISO={format_epoch(start_epoch)}",
                f"END_EPOCH={end_epoch}",
                f"END_ISO={format_epoch(end_epoch)}",
                f"K6_LOG={log_path}",
                f"STATUS={result.returncode}",
                f"COMMAND={' '.join(command)}",
                "",
            ]
        )
    )
    if result.returncode != 0:
        raise SystemExit(f"k6 failed for {scenario.slug} with exit code {result.returncode}; see {log_path}")
    return start_epoch, end_epoch, log_path, env_path


def fetch_traces(args: argparse.Namespace, operation: str, start_epoch: int, end_epoch: int, limit: int | None = None) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode(
        {
            "service": "mongot",
            "operation": operation,
            "start": max(0, start_epoch - 5) * 1_000_000,
            "end": (end_epoch + 30) * 1_000_000,
            "limit": limit or args.trace_limit,
        }
    )
    url = f"{args.jaeger_url.rstrip('/')}/api/traces?{params}"
    with urllib.request.urlopen(url, timeout=180) as response:
        data = json.load(response)
    return data.get("data", [])


def tags(span: dict[str, Any]) -> dict[str, Any]:
    return {tag.get("key"): tag.get("value") for tag in span.get("tags", [])}


def operation_map(trace: dict[str, Any], command_end: int | None) -> tuple[dict[str, list[float]], dict[str, list[float]], dict[str, list[float]]]:
    all_map: dict[str, list[float]] = {}
    initial: dict[str, list[float]] = {}
    later: dict[str, list[float]] = {}
    for span in trace.get("spans", []):
        name = span.get("operationName", "")
        duration = float(span.get("duration") or 0)
        all_map.setdefault(name, []).append(duration)
        target = initial if command_end is not None and span.get("startTime", 0) <= command_end else later
        target.setdefault(name, []).append(duration)
    return all_map, initial, later


def sum_ops(span_map: dict[str, list[float]], operations: tuple[str, ...] | list[str]) -> float:
    return sum(sum(span_map.get(operation, [])) for operation in operations)


def calculate_search_metrics(traces: list[dict[str, Any]], operation: str) -> dict[str, Any]:
    values = {segment.key: [] for segment in SEARCH_SEGMENTS}
    values["root"] = []
    values["command"] = []
    later = {"collect_more": [], "materialize_bson": [], "advance_batch": []}

    for trace in traces:
        command_spans = [span for span in trace.get("spans", []) if span.get("operationName") == "mongot.search.command"]
        command_end = None
        if command_spans:
            command_end = command_spans[0].get("startTime", 0) + command_spans[0].get("duration", 0)
        all_map, initial, later_map = operation_map(trace, command_end)

        root_us = sum_ops(all_map, (operation,))
        command_us = sum_ops(all_map, ("mongot.search.command",))
        values["root"].append(root_us)
        values["command"].append(command_us)

        for segment in SEARCH_SEGMENTS:
            if not segment.computed:
                values[segment.key].append(sum_ops(initial, segment.spans))

        cursor_create = sum_ops(initial, ("mongot.search.create_and_register_cursor", "mongot.search.create_cursor"))
        cursor_children = sum_ops(
            initial,
            (
                "mongot.cursor.select_index_manager",
                "mongot.cursor.choose_batch_size",
                "mongot.cursor.instantiate_cursor",
                "mongot.cursor.register_active_cursor",
                "mongot.cursor.register_index_mapping",
                "mongot.lucene.prepare_search_reader_query",
                "mongot.lucene.search_index_reader.query",
            ),
        )
        values["cursor_orchestration"].append(max(0, cursor_create - cursor_children))

        reader_query = sum_ops(initial, ("mongot.lucene.prepare_search_reader_query", "mongot.lucene.search_index_reader.query"))
        reader_children = sum_ops(
            initial,
            (
                "mongot.lucene.open_index_searcher",
                "mongot.lucene.build_query",
                "mongot.lucene.create_search_query",
                "mongot.lucene.collect_initial_top_docs",
                "mongot.lucene.initial_top_docs",
                "mongot.lucene.vector_collect_candidates",
                "mongot.lucene.prepare_highlights",
                "mongot.lucene.prepare_highlighter",
                "mongot.lucene.prepare_score_details",
                "mongot.lucene.prepare_pagination_sort_context",
                "mongot.lucene.build_sort",
                "mongot.lucene.create_sort",
                "mongot.lucene.create_search_manager",
                "mongot.lucene.create_result_producer",
            ),
        )
        values["reader_orchestration"].append(max(0, reader_query - reader_children))

        first_batch = sum_ops(initial, ("mongot.search.load_first_cursor_batch",))
        advance = sum_ops(initial, ("mongot.cursor.advance_batch_producer",))
        materialize = sum_ops(initial, ("mongot.lucene.materialize_bson_documents", "mongot.lucene.materialize_results"))
        values["batch_orchestration"].append(max(0, first_batch - advance - materialize))

        top_level = sum_ops(
            initial,
            (
                "mongot.search.prepare_query_context",
                "mongot.search.parse_query_bson",
                "mongot.search.parse_query",
                "Query.fromBson",
                "mongot.search.lookup_index_catalog",
                "mongot.search.resolve_index",
                "mongot.search.parse_cursor_options",
                "mongot.search.validate_query_options",
                "mongot.search.validate_query",
                "mongot.search.prepare_response_mode",
                "mongot.search.build_first_batch",
                "mongot.search.prepare_response_document",
                "mongot.search.encode_response_bson",
                "mongot.search.serialize_batch",
            ),
        )
        values["command_orchestration"].append(max(0, command_us - top_level))
        values["stream_lifecycle"].append(max(0, root_us - command_us))

        later["collect_more"].append(
            sum_ops(later_map, ("mongot.lucene.collect_more_top_docs", "mongot.lucene.get_more_top_docs", "mongot.lucene.vector_get_more_top_docs"))
        )
        later["materialize_bson"].append(sum_ops(later_map, ("mongot.lucene.materialize_bson_documents", "mongot.lucene.materialize_results")))
        later["advance_batch"].append(sum_ops(later_map, ("mongot.cursor.advance_batch_producer",)))

    return {"values": values, "later": later}


def sample_search_payload(traces: list[dict[str, Any]]) -> dict[str, str]:
    sample = {
        "trace_id": "",
        "operation_id": "",
        "index_name": "",
        "query_type": "",
        "lucene_query": "",
        "lucene_query_class": "",
        "reader_docs": "",
        "reader_segments": "",
        "result_sample_count": "",
        "vector_limit": "",
        "vector_search_type": "",
    }
    for trace in traces:
        sample["trace_id"] = trace.get("traceID", sample["trace_id"])
        for span in trace.get("spans", []):
            span_tags = tags(span)
            name = span.get("operationName")
            if name == "mongot.search.command":
                sample["operation_id"] = str(span_tags.get("mongot.operation.id", sample["operation_id"]))
                sample["index_name"] = str(span_tags.get("mongot.search.index.name", sample["index_name"]))
                sample["query_type"] = str(span_tags.get("mongot.search.query.type", sample["query_type"]))
            elif name == "mongot.lucene.build_query":
                sample["lucene_query"] = str(span_tags.get("mongot.lucene.query", sample["lucene_query"]))
                sample["lucene_query_class"] = str(span_tags.get("mongot.lucene.query.class", sample["lucene_query_class"]))
            elif name == "mongot.lucene.open_index_searcher":
                sample["reader_docs"] = str(span_tags.get("mongot.lucene.reader.num_docs", sample["reader_docs"]))
                sample["reader_segments"] = str(span_tags.get("mongot.lucene.reader.segment_count", sample["reader_segments"]))
            elif name == "mongot.lucene.materialize_bson_documents":
                sample["result_sample_count"] = str(span_tags.get("mongot.result.sample.count", sample["result_sample_count"]))
            elif name == "mongot.lucene.vector_initial_top_docs":
                sample["vector_limit"] = str(span_tags.get("mongot.vector.limit", sample["vector_limit"]))
                sample["vector_search_type"] = str(span_tags.get("mongot.vector.search.type", sample["vector_search_type"]))
        if sample["lucene_query"] and sample["result_sample_count"]:
            break
    return sample


def search_component_summary(traces: list[dict[str, Any]], operation: str) -> list[dict[str, Any]]:
    components: dict[str, dict[str, Any]] = {}
    for trace in traces:
        index_name = "unknown"
        root_us = 0.0
        command_us = 0.0
        spans_by_name: dict[str, list[float]] = {}
        for span in trace.get("spans", []):
            name = span.get("operationName", "")
            duration = float(span.get("duration") or 0)
            spans_by_name.setdefault(name, []).append(duration)
            if name == operation:
                root_us += duration
            if name == "mongot.search.command":
                command_us += duration
                index_name = str(tags(span).get("mongot.search.index.name", index_name))
        component = components.setdefault(
            index_name,
            {
                "index": index_name,
                "traces": 0,
                "root": [],
                "command": [],
                "parse": [],
                "build_query": [],
                "text_collect": [],
                "vector_collect": [],
                "materialize": [],
            },
        )
        component["traces"] += 1
        component["root"].append(root_us)
        component["command"].append(command_us)
        component["parse"].append(sum_ops(spans_by_name, ("mongot.search.parse_query_bson", "mongot.search.parse_query", "Query.fromBson")))
        component["build_query"].append(sum_ops(spans_by_name, ("mongot.lucene.build_query", "mongot.lucene.create_search_query")))
        component["text_collect"].append(sum_ops(spans_by_name, ("mongot.lucene.collect_initial_top_docs", "mongot.lucene.initial_top_docs")))
        component["vector_collect"].append(sum_ops(spans_by_name, ("mongot.lucene.vector_collect_candidates",)))
        component["materialize"].append(sum_ops(spans_by_name, ("mongot.lucene.materialize_bson_documents", "mongot.lucene.materialize_results")))

    summary = []
    for component in components.values():
        summary.append(
            {
                "index": component["index"],
                "traces": component["traces"],
                "root": median(component["root"]),
                "command": median(component["command"]),
                "parse": median(component["parse"]),
                "build_query": median(component["build_query"]),
                "text_collect": median(component["text_collect"]),
                "vector_collect": median(component["vector_collect"]),
                "materialize": median(component["materialize"]),
            }
        )
    return sorted(summary, key=lambda item: item["traces"], reverse=True)


def collect_mutation_metrics(args: argparse.Namespace, start_epoch: int, end_epoch: int) -> dict[str, Any]:
    values = {segment.key: [] for segment in MUTATION_SEGMENTS}
    trace_counts: dict[str, int] = {}
    examples: dict[str, str] = {}
    attrs: dict[str, list[dict[str, Any]]] = {segment.key: [] for segment in MUTATION_SEGMENTS}

    for segment in MUTATION_SEGMENTS:
        operation = segment.spans[0]
        traces = fetch_traces(args, operation, start_epoch, end_epoch, limit=args.trace_limit)
        trace_counts[segment.key] = len(traces)
        for trace in traces:
            if segment.key not in examples:
                examples[segment.key] = trace.get("traceID", "")
            for span in trace.get("spans", []):
                if span.get("operationName") != operation:
                    continue
                span_tags = tags(span)
                if segment.key == "document_insert" and span_tags.get("mongot.indexing.event.type") != "INSERT":
                    continue
                if segment.key == "document_delete" and span_tags.get("mongot.indexing.event.type") != "DELETE":
                    continue
                values[segment.key].append(float(span.get("duration") or 0))
                attrs[segment.key].append(span_tags)
    return {"values": values, "trace_counts": trace_counts, "examples": examples, "attrs": attrs}


def draw_chart(path: Path, title: str, subtitle: str, cards: list[tuple[str, str]], rows: list[tuple[str, float]], footer: str) -> None:
    width, height = 1700, 1120
    image = Image.new("RGB", (width, height), "#111217")
    draw = ImageDraw.Draw(image)
    text = "#D8DCE7"
    muted = "#A7ADBC"
    panel = "#181B20"
    line = "#2B303B"
    colors = ["#73BF69", "#F2CC0C", "#5794F2", "#FF9830", "#F2495C", "#B877D9", "#37872D", "#FADE2A", "#3274D9", "#FF6B00", "#C4162A", "#8F3BB8"]

    draw.text((36, 28), title, fill=text, font=load_font(34, True))
    draw.text((36, 72), subtitle, fill=muted, font=load_font(19))

    card_width = 260
    for index, (label, value) in enumerate(cards[:6]):
        x = 36 + index * (card_width + 14)
        y = 118
        draw.rounded_rectangle((x, y, x + card_width, y + 94), radius=8, fill=panel, outline=line)
        draw.text((x + 16, y + 14), label, fill=muted, font=load_font(16, True))
        draw.text((x + 16, y + 46), value, fill="#73BF69", font=load_font(27, True))

    max_value = max((value for _, value in rows), default=1)
    y = 272
    draw.text((36, 238), "Median span breakdown", fill=text, font=load_font(25, True))
    for index, (label, value) in enumerate(rows[:24]):
        bar_width = int(1050 * value / max_value) if max_value else 0
        draw.text((36, y - 4), label, fill=text, font=load_font(18))
        draw.rectangle((420, y, 420 + 1050, y + 20), fill="#20242C")
        draw.rectangle((420, y, 420 + max(1, bar_width), y + 20), fill=colors[index % len(colors)])
        draw.text((1490, y - 4), fmt_us(value), fill=muted, font=load_font(18))
        y += 34

    draw.text((36, height - 56), footer, fill=muted, font=load_font(17))
    image.save(path)


def search_k6_summary(k6_text: str) -> dict[str, str]:
    requests_line = line_value(k6_text, "search_requests")
    request_parts = requests_line.split()
    http_line = line_value(k6_text, "search_http_time_ms")
    mongo_line = line_value(k6_text, "search_mongodb_time_ms")
    java_line = line_value(k6_text, "search_java_non_mongo_time_ms")
    return {
        "requests": request_parts[0] if request_parts else "0",
        "throughput": request_parts[1].replace("/s", " req/s") if len(request_parts) > 1 else "",
        "failed": line_value(k6_text, "http_req_failed"),
        "http_med": metric_number(http_line, "med"),
        "http_p95": metric_number(http_line, "p(95)"),
        "mongo_med": metric_number(mongo_line, "med"),
        "mongo_p95": metric_number(mongo_line, "p(95)"),
        "java_med": metric_number(java_line, "med"),
    }


def mutation_k6_summary(k6_text: str) -> dict[str, str]:
    requests_line = line_value(k6_text, "mutation_requests")
    insert_line = line_value(k6_text, "insert_requests")
    delete_line = line_value(k6_text, "delete_requests")
    request_parts = requests_line.split()
    insert_parts = insert_line.split()
    delete_parts = delete_line.split()
    http_line = line_value(k6_text, "mutation_http_time_ms")
    return {
        "requests": request_parts[0] if request_parts else "0",
        "throughput": request_parts[1].replace("/s", " req/s") if len(request_parts) > 1 else "",
        "inserts": insert_parts[0] if insert_parts else "0",
        "deletes": delete_parts[0] if delete_parts else "0",
        "failed": line_value(k6_text, "http_req_failed"),
        "http_med": metric_number(http_line, "med"),
        "http_p95": metric_number(http_line, "p(95)"),
    }


def write_search_report(
    path: Path,
    scenario: Scenario,
    run_id: str,
    start_epoch: int,
    end_epoch: int,
    k6_log: Path,
    traces: list[dict[str, Any]],
    metrics: dict[str, Any],
    image_path: Path,
) -> dict[str, Any]:
    k6 = search_k6_summary(k6_log.read_text())
    values = metrics["values"]
    root_median = median(values["root"])
    command_median = median(values["command"])
    sample = sample_search_payload(traces)
    component_summary = search_component_summary(traces, scenario.operation)
    component_rows = [
        "| {index} | {traces:,} | {root} | {command} | {parse} | {build_query} | {text_collect} | {vector_collect} | {materialize} |".format(
            index=component["index"],
            traces=component["traces"],
            root=fmt_us(component["root"]),
            command=fmt_us(component["command"]),
            parse=fmt_us(component["parse"]),
            build_query=fmt_us(component["build_query"]),
            text_collect=fmt_us(component["text_collect"]),
            vector_collect=fmt_us(component["vector_collect"]),
            materialize=fmt_us(component["materialize"]),
        )
        for component in component_summary
    ]
    rows: list[str] = []
    chart_rows: list[tuple[str, float]] = []
    for segment in SEARCH_SEGMENTS:
        value = median(values.get(segment.key, []))
        if value <= 0 and segment.key not in {"stream_lifecycle", "command_orchestration"}:
            continue
        denominator = root_median if segment.key == "stream_lifecycle" else command_median
        pct_label = "overall" if segment.key == "stream_lifecycle" else "of command"
        rows.append(
            "| {label} | {time} | {pct_value} {pct_label} | `{spans}` | {code} | {desc} |".format(
                label=segment.label,
                time=fmt_us(value),
                pct_value=pct(value, denominator),
                pct_label=pct_label,
                spans="`, `".join(segment.spans),
                code=segment_code(segment),
                desc=segment.description,
            )
        )
        chart_rows.append((segment.label, value))

    chart_rows = sorted(chart_rows, key=lambda item: item[1], reverse=True)
    cards = [
        ("k6 throughput", k6["throughput"]),
        ("HTTP median", f"{k6['http_med']} ms"),
        ("MongoDB median", f"{k6['mongo_med']} ms"),
        ("stream median", fmt_us(root_median)),
        ("command median", fmt_us(command_median)),
        ("traces", f"{len(traces):,}"),
    ]
    draw_chart(
        image_path,
        scenario.title,
        f"{format_epoch(start_epoch)} to {format_epoch(end_epoch)} | {len(traces):,} Jaeger traces",
        cards,
        chart_rows,
        f"Later getMore medians: collect {fmt_us(median(metrics['later']['collect_more']))}, materialize {fmt_us(median(metrics['later']['materialize_bson']))}, advance {fmt_us(median(metrics['later']['advance_batch']))}.",
    )

    content = f"""# MongoT Trace Breakdown: {scenario.title}

Run window: {format_epoch(start_epoch)} to {format_epoch(end_epoch)}.

Command:

```sh
k6 run -e K6_VUS={k6.get('vus', '') or 'see run env'} -e K6_DURATION={k6.get('duration', '') or 'see run env'} {' '.join(f'-e {key}={value}' for key, value in scenario.env.items())} {scenario.script}
```

Scenario:

{scenario.description}

![{scenario.title} chart](./{image_path.name})

## Run Summary

| Metric | Value |
| --- | ---: |
| k6 requests | {k6['requests']} |
| k6 throughput | {k6['throughput']} |
| k6 failures | {k6['failed']} |
| HTTP median | {k6['http_med']} ms |
| HTTP p95 | {k6['http_p95']} ms |
| App-reported MongoDB median | {k6['mongo_med']} ms |
| App-reported MongoDB p95 | {k6['mongo_p95']} ms |
| App Java non-Mongo median | {k6['java_med']} ms |
| Jaeger traces sampled | {len(traces):,} |
| Root trace operation | `{scenario.operation}` |
| Median stream span | {fmt_us(root_median)} |
| Median `mongot.search.command` | {fmt_us(command_median)} |

## Representative Trace Attributes

| Attribute | Value |
| --- | --- |
| Trace id | `{sample['trace_id']}` |
| Operation id | `{sample['operation_id']}` |
| Search index | `{sample['index_name']}` |
| Query type | `{sample['query_type']}` |
| Lucene query class | `{sample['lucene_query_class']}` |
| Lucene query | `{sample['lucene_query']}` |
| Reader docs / segments | {sample['reader_docs']} docs / {sample['reader_segments']} segments |
| Vector limit / type | {sample['vector_limit']} / {sample['vector_search_type']} |
| Result samples attached | {sample['result_sample_count']} |

## Component Split

This groups sampled traces by `mongot.search.index.name`. It is especially useful for combined/rank-fusion workloads because the text and vector sub-pipelines both appear as MongoT search streams.

| Index | Traces | Stream median | Command median | Parse BSON | Build query | Text collect | Vector collect | Materialize BSON |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
{chr(10).join(component_rows)}

## Trace Segments

| Segment | Median | Share | Span(s) | Code | Description |
| --- | ---: | ---: | --- | --- | --- |
{chr(10).join(rows)}

## Notes

The root stream span is the most representative end-to-end MongoT view because it includes gRPC stream lifecycle, the initial command span, and later cursor/getMore work in the same stream. The command-phase percentages show only the initial `mongot.search.command` span so Lucene/query/materialization work remains visible.
"""
    path.write_text(content)
    return {
        "scenario": scenario.slug,
        "title": scenario.title,
        "report": str(path),
        "image": str(image_path),
        "requests": k6["requests"],
        "throughput": k6["throughput"],
        "http_med": k6["http_med"],
        "mongo_med": k6["mongo_med"],
        "stream_median_us": root_median,
        "command_median_us": command_median,
        "trace_count": len(traces),
    }


def write_mutation_report(
    path: Path,
    scenario: Scenario,
    start_epoch: int,
    end_epoch: int,
    k6_log: Path,
    metrics: dict[str, Any],
    image_path: Path,
) -> dict[str, Any]:
    k6 = mutation_k6_summary(k6_log.read_text())
    values = metrics["values"]
    rows: list[str] = []
    chart_rows: list[tuple[str, float]] = []
    for segment in MUTATION_SEGMENTS:
        value = median(values.get(segment.key, []))
        count = len(values.get(segment.key, []))
        trace_count = metrics["trace_counts"].get(segment.key, 0)
        if count == 0 and trace_count == 0:
            continue
        rows.append(
            "| {label} | {time} | {count} | {trace_count} | `{spans}` | {code} | {desc} |".format(
                label=segment.label,
                time=fmt_us(value),
                count=count,
                trace_count=trace_count,
                spans="`, `".join(segment.spans),
                code=segment_code(segment),
                desc=segment.description,
            )
        )
        if value > 0:
            chart_rows.append((segment.label, value))

    total_indexing_traces = sum(metrics["trace_counts"].values())
    cards = [
        ("mutations/sec", k6["throughput"]),
        ("requests", k6["requests"]),
        ("inserts", k6["inserts"]),
        ("deletes", k6["deletes"]),
        ("HTTP median", f"{k6['http_med']} ms"),
        ("trace hits", f"{total_indexing_traces:,}"),
    ]
    draw_chart(
        image_path,
        scenario.title,
        f"{format_epoch(start_epoch)} to {format_epoch(end_epoch)} | indexing/update spans from Jaeger",
        cards,
        sorted(chart_rows, key=lambda item: item[1], reverse=True),
        "Mutation traces are asynchronous: API writes go to mongod, then MongoT observes change streams and updates Lucene in background scheduler traces.",
    )

    content = f"""# MongoT Trace Breakdown: {scenario.title}

Run window: {format_epoch(start_epoch)} to {format_epoch(end_epoch)}.

Command:

```sh
k6 run -e K6_VUS=see-run-env -e K6_DURATION=see-run-env {' '.join(f'-e {key}={value}' for key, value in scenario.env.items())} {scenario.script}
```

Scenario:

{scenario.description}

![{scenario.title} chart](./{image_path.name})

## Run Summary

| Metric | Value |
| --- | ---: |
| mutation requests | {k6['requests']} |
| mutation throughput | {k6['throughput']} |
| inserts | {k6['inserts']} |
| deletes | {k6['deletes']} |
| failures | {k6['failed']} |
| HTTP median | {k6['http_med']} ms |
| HTTP p95 | {k6['http_p95']} ms |
| indexing trace hits | {total_indexing_traces:,} |

## Trace Segments

| Segment | Median | Spans observed | Traces matched | Span(s) | Code | Description |
| --- | ---: | ---: | ---: | --- | --- | --- |
{chr(10).join(rows)}

## Notes

Insert/delete API calls do not become children of a single client trace inside MongoT, because the API writes land in MongoDB first. MongoT then observes those writes through steady-state change streams. The useful correlation is therefore by run window, namespace/index generation attributes, `mongot.indexing.event.type`, and the Lucene writer spans emitted by the background indexing scheduler.

The paired mutation script inserts a synthetic image and deletes that same generated id, so it exercises insert and delete update paths without intentionally deleting COCO source records.
"""
    path.write_text(content)
    return {
        "scenario": scenario.slug,
        "title": scenario.title,
        "report": str(path),
        "image": str(image_path),
        "requests": k6["requests"],
        "throughput": k6["throughput"],
        "http_med": k6["http_med"],
        "trace_count": total_indexing_traces,
    }


def write_index_report(output_dir: Path, run_prefix: str, summaries: list[dict[str, Any]]) -> Path:
    path = output_dir / f"trace-breakdown-summary-{run_prefix}.md"
    rows = []
    for summary in summaries:
        rows.append(
            f"| {summary['title']} | [{Path(summary['report']).name}](./{Path(summary['report']).name}) | "
            f"[{Path(summary['image']).name}](./{Path(summary['image']).name}) | {summary.get('requests', '')} | "
            f"{summary.get('throughput', '')} | {summary.get('http_med', '')} ms | {summary.get('trace_count', '')} |"
        )
    path.write_text(
        "# MongoT Trace Scenario Breakdown Summary\n\n"
        f"Generated: {format_epoch(int(time.time()))}.\n\n"
        "| Scenario | Report | Chart | Requests | Throughput | HTTP median | Trace hits |\n"
        "| --- | --- | --- | ---: | ---: | ---: | ---: |\n"
        + "\n".join(rows)
        + "\n"
    )
    return path


def main() -> int:
    args = parse_args()
    args.repo_root = args.repo_root.resolve()
    args.coco_dir = args.coco_dir.resolve()
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    available = scenarios_for(args)
    requested = [token.strip() for token in args.scenarios.split(",") if token.strip()]
    unknown = [token for token in requested if token not in available]
    if unknown:
        raise SystemExit(f"Unknown scenario(s): {', '.join(unknown)}")

    summaries: list[dict[str, Any]] = []
    for index, scenario_id in enumerate(requested, start=1):
        scenario = available[scenario_id]
        run_id = f"{args.run_prefix}-{index:02d}-{scenario.slug}"
        print(f"scenario_start={scenario.slug} title={scenario.title}", flush=True)
        if args.skip_k6:
            raise SystemExit("--skip-k6 is intentionally not supported for the multi-scenario runner yet")
        start_epoch, end_epoch, k6_log, _ = run_k6(args, scenario, run_id)

        if scenario.kind == "search":
            traces = fetch_traces(args, scenario.operation, start_epoch, end_epoch)
            if not traces:
                raise SystemExit(f"No Jaeger traces found for {scenario.title} ({scenario.operation})")
            metrics = calculate_search_metrics(traces, scenario.operation)
            report_path = args.output_dir / f"trace-breakdown-{scenario.slug}-{run_id}.md"
            image_path = args.output_dir / f"trace-breakdown-{scenario.slug}-{run_id}.png"
            summary = write_search_report(report_path, scenario, run_id, start_epoch, end_epoch, k6_log, traces, metrics, image_path)
        else:
            metrics = collect_mutation_metrics(args, start_epoch, end_epoch)
            report_path = args.output_dir / f"trace-breakdown-{scenario.slug}-{run_id}.md"
            image_path = args.output_dir / f"trace-breakdown-{scenario.slug}-{run_id}.png"
            summary = write_mutation_report(report_path, scenario, start_epoch, end_epoch, k6_log, metrics, image_path)

        summaries.append(summary)
        print(f"scenario_done={scenario.slug} report={summary['report']} image={summary['image']}", flush=True)

    index_path = write_index_report(args.output_dir, args.run_prefix, summaries)
    print(f"summary={index_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
