# MongoT Package Tour

Complexity is a subjective `1-10` score based on package size, internal breadth, and how central the package appears in the codebase. Linked packages are other major packages directly imported from that package's source.

The package descriptions now include the runtime picture learned from detailed OpenTelemetry traces gathered with `DETAILED_TRACE_SPANS=true` against a generic client workload. Those traces show the main search stream, the initial command span, Lucene execution spans, BSON materialization, cursor/getMore work, and asynchronous indexing updates from MongoDB change streams.

| Package | Description | Complexity | Linked Major Packages |
| --- | --- | ---: | --- |
| `com.xgen.mongot.blobstore` | Minimal blob-storage integration surface; currently mostly a placeholder/error boundary. | 1 | `com.xgen.mongot.util` |
| `com.xgen.mongot.catalog` | Local index catalog abstractions and implementations used to resolve/search index state. | 2 | `com.xgen.mongot.index` |
| `com.xgen.mongot.catalogservice` | Metadata service layer for authoritative index definitions, per-server index stats, and server heartbeats stored in the internal metadata database. | 4 | `com.xgen.mongot.util`, `com.xgen.mongot.index`, `com.xgen.mongot.replication` |
| `com.xgen.mongot.community` | Community-edition entrypoint and top-level assembly/bootstrap wiring. | 1 | `com.xgen.mongot.util`, `com.xgen.mongot.config`, `com.xgen.mongot.logging` |
| `com.xgen.mongot.config` | Configuration models, validation, providers, change planning, and config-management workflow for mongot subsystems. | 7 | `com.xgen.mongot.util`, `com.xgen.mongot.index`, `com.xgen.mongot.replication`, `com.xgen.mongot.featureflag`, `com.xgen.mongot.metrics`, `com.xgen.mongot.catalog`, `com.xgen.mongot.catalogservice`, `com.xgen.mongot.embedding`, `com.xgen.mongot.server`, `com.xgen.mongot.monitor`, `com.xgen.mongot.cursor`, `com.xgen.mongot.lifecycle`, `com.xgen.mongot.logging` |
| `com.xgen.mongot.cursor` | Cursor domain model, managers, batching, and serialization for paged search results / getMore flows. Detailed traces expose cursor create/register, first-batch loading, batch producer advancement, and later cursor work inside the same gRPC search stream. | 5 | `com.xgen.mongot.index`, `com.xgen.mongot.util`, `com.xgen.mongot.trace`, `com.xgen.mongot.catalog`, `com.xgen.mongot.metrics` |
| `com.xgen.mongot.embedding` | Embedding-provider integration, request context, auto-embedding helpers, and materialized-view support for vector workflows. | 6 | `com.xgen.mongot.util`, `com.xgen.mongot.index`, `com.xgen.mongot.metrics`, `com.xgen.mongot.replication` |
| `com.xgen.mongot.featureflag` | Static and dynamic feature flag definitions plus runtime flag registry/config. | 2 | `com.xgen.mongot.util`, `com.xgen.mongot.index` |
| `com.xgen.mongot.index` | Core search/vector engine: index definitions, ingestion, Lucene integration, query execution, result shaping, and index status/metadata. Trace breakdowns split this package into concrete runtime work: build Lucene query, collect text hits, collect vector candidates, materialize BSON, update/delete Lucene documents, commit, and refresh searchers. | 10 | `com.xgen.mongot.util`, `com.xgen.mongot.featureflag`, `com.xgen.mongot.metrics`, `com.xgen.mongot.cursor`, `com.xgen.mongot.embedding`, `com.xgen.mongot.monitor`, `com.xgen.mongot.trace`, `com.xgen.mongot.server`, `com.xgen.proto`, `com.xgen.mongot.blobstore`, `com.xgen.mongot.config`, `com.xgen.mongot.logging` |
| `com.xgen.mongot.lifecycle` | Startup/shutdown lifecycle coordination, especially around index lifecycle management. | 3 | `com.xgen.mongot.index`, `com.xgen.mongot.util`, `com.xgen.mongot.replication`, `com.xgen.mongot.catalog`, `com.xgen.mongot.metrics`, `com.xgen.mongot.blobstore`, `com.xgen.mongot.monitor` |
| `com.xgen.mongot.logging` | Structured logging helpers and JSON log-format customization. | 1 | None |
| `com.xgen.mongot.metrics` | Metrics abstractions plus FTDC collection/reporting infrastructure. | 4 | `com.xgen.mongot.util`, `com.xgen.mongot.index` |
| `com.xgen.mongot.monitor` | Disk and replication-state monitoring, gates, and hysteresis controls used to protect service behavior under stress. | 3 | `com.xgen.mongot.util`, `com.xgen.mongot.config`, `com.xgen.mongot.metrics` |
| `com.xgen.mongot.replication` | MongoDB replication pipeline, including initial sync, steady-state change-stream processing, durability, and indexing work scheduling. Mutation traces show this path as change stream batch, decode batch, decoded document event, indexing batch, Lucene writer update/delete, commit, and searcher refresh. | 9 | `com.xgen.mongot.util`, `com.xgen.mongot.index`, `com.xgen.mongot.metrics`, `com.xgen.mongot.embedding`, `com.xgen.mongot.logging`, `com.xgen.mongot.featureflag`, `com.xgen.mongot.catalog`, `com.xgen.mongot.cursor`, `com.xgen.mongot.monitor` |
| `com.xgen.mongot.server` | External server surface: gRPC/command handling, protocol plumbing, request routing, and streaming responses. Search root spans are named by namespace and operation, such as `mongodb.CommandService/<database>.<collection>/search`, so traces can be correlated back to the client request shape without implying that the Java driver connects to MongoT directly. | 8 | `com.xgen.mongot.util`, `com.xgen.mongot.index`, `com.xgen.mongot.cursor`, `com.xgen.mongot.config`, `com.xgen.mongot.catalogservice`, `com.xgen.mongot.catalog`, `com.xgen.mongot.embedding`, `com.xgen.mongot.metrics`, `com.xgen.mongot.featureflag`, `com.xgen.mongot.trace` |
| `com.xgen.mongot.trace` | OpenTelemetry tracing helpers, payload attribute guards, detailed-span toggles, and trace parsing utilities. With detailed tracing enabled, important spans include full input BSON, Lucene query string/class, sampled BSON result documents, index/update event metadata, and operation ids for correlation. | 3 | None |
| `com.xgen.mongot.util` | Shared foundation code used across mongot: BSON/proto conversion, concurrency helpers, collections, versioning, and general utilities. | 8 | `com.xgen.proto`, `com.xgen.mongot.metrics`, `com.xgen.mongot.logging` |
| `com.xgen.proto` | BSON-aware protobuf runtime plus code-generation plugin for BSON-capable protobuf messages. | 4 | None |

## `com.xgen.mongot.index` Drilldown

| Package | Description | Complexity | Linked Major Packages |
| --- | --- | ---: | --- |
| `com.xgen.mongot.index.analyzer` | Analyzer builders, providers, factories, and language-specific tokenization plumbing for index definitions and query-time analysis. | 6 | `com.xgen.mongot.index.definition`, `com.xgen.mongot.index.lucene`, `com.xgen.mongot.index.path`, `com.xgen.mongot.index.query` |
| `com.xgen.mongot.index.autoembedding` | Auto-embedding and materialized-view index helpers that derive generated fields and coordinate embedding-oriented index metadata. | 4 | `com.xgen.mongot.index.definition`, `com.xgen.mongot.index.mongodb`, `com.xgen.mongot.index.status`, `com.xgen.mongot.index.version`, `com.xgen.mongot.index.analyzer`, `com.xgen.mongot.index.query` |
| `com.xgen.mongot.index.blobstore` | Snapshotting hooks for persisting and restoring index state through blob storage. | 2 | `com.xgen.mongot.index.version` |
| `com.xgen.mongot.index.definition` | Core schema model for search, vector, and view indexes, including field definitions, options, and validation logic. | 8 | `com.xgen.mongot.index.version`, `com.xgen.mongot.index.analyzer`, `com.xgen.mongot.index.query`, `com.xgen.mongot.index.lucene`, `com.xgen.mongot.index.path` |
| `com.xgen.mongot.index.ingestion` | BSON document processing, field extraction, and ingestion-time transforms that feed Lucene indexing. Insert/update traces show this as `build document block`, immediately before Lucene `IndexWriter.updateDocuments`. | 5 | `com.xgen.mongot.index.definition`, `com.xgen.mongot.index.lucene` |
| `com.xgen.mongot.index.lucene` | Largest execution layer: Lucene-backed indexing, search, highlighting, result shaping, commit management, and searcher orchestration. Trace runs distinguish setup from actual Lucene calls: open searcher, build query, collect hits/vector candidates, materialize BSON, updateDocuments, deleteDocuments, commit, and SearcherManager refresh. | 10 | `com.xgen.mongot.index.query`, `com.xgen.mongot.index.definition`, `com.xgen.mongot.index.analyzer`, `com.xgen.mongot.index.path`, `com.xgen.mongot.index.ingestion`, `com.xgen.mongot.index.version`, `com.xgen.mongot.index.synonym`, `com.xgen.mongot.index.status`, `com.xgen.mongot.index.blobstore` |
| `com.xgen.mongot.index.mongodb` | Narrow MongoDB-facing helpers for materialized-view writes and index-related metrics/state propagation. | 2 | `com.xgen.mongot.index.lucene`, `com.xgen.mongot.index.status`, `com.xgen.mongot.index.version` |
| `com.xgen.mongot.index.path` | Shared path abstractions for dotted field-path parsing and traversal across schema and query code. | 2 | None |
| `com.xgen.mongot.index.query` | Query AST, operators, collectors, pagination, score shaping, and translation from request semantics into Lucene execution. Search traces expose BSON parse time in this layer and attach the generated Lucene query class/string on the build-query span. | 9 | `com.xgen.mongot.index.path`, `com.xgen.mongot.index.definition`, `com.xgen.mongot.index.lucene` |
| `com.xgen.mongot.index.status` | Index and synonym status enums/models used to expose lifecycle and readiness state. | 2 | None |
| `com.xgen.mongot.index.synonym` | Synonym mapping models, registries, and status tracking integrated with Lucene query behavior. | 3 | `com.xgen.mongot.index.status`, `com.xgen.mongot.index.definition` |
| `com.xgen.mongot.index.version` | Index format/version identifiers, generation metadata, and compatibility/capability checks. | 3 | None |

## Runtime Trace View

The detailed trace runs add a useful second lens to the package map: package structure explains ownership, while traces show the actual execution path for search and index updates. The generated reports live under [`load-results/breakdown-20260426-23`](load-results/breakdown-20260426-23/trace-breakdown-summary-full-20260426-225217.md).

### Search Request Path

For search workloads, the root MongoT span is the gRPC command stream. It is named by namespace and operation, for example:

```text
mongodb.CommandService/<database>.<collection>/search
```

That root stream includes the initial command span (`mongot.search.command` for text search or `mongot.vector_search.command` for vector search), response-stream handling, client consumption, and later cursor/getMore activity in the same stream. The command span is the best view of the initial MongoT command work; the stream span is the better end-to-end MongoT latency view.

The main package flow for text and vector search is:

| Phase | Main Package(s) | Important Span(s) | What It Means |
| --- | --- | --- | --- |
| Stream entry | `com.xgen.mongot.server` | `mongodb.CommandService/<namespace>/<operation>` | gRPC command stream lifecycle, command dispatch, response observer work, and stream close/cleanup. |
| Command handling | `com.xgen.mongot.server`, `com.xgen.mongot.trace` | `mongot.search.command`, `mongot.vector_search.command` | Command root with operation id, namespace, command name, full input BSON, and top-level command attributes. |
| Query context | `com.xgen.mongot.server.command.search`, `com.xgen.mongot.featureflag` | `mongot.search.prepare_query_context`, `mongot.vector_search.validate_query` | Resolves query optimization flags and dynamic feature state before Lucene execution. |
| BSON parse | `com.xgen.mongot.index.query` | `mongot.search.parse_query_bson`, `mongot.vector_search.parse_query`, `Query.fromBson` | Converts incoming search/vector BSON into MongoT query model objects. This is not Lucene execution. |
| Catalog lookup | `com.xgen.mongot.catalog`, `com.xgen.mongot.index` | `mongot.search.lookup_index_catalog` | Resolves the named search/vector index from MongoT's initialized in-memory catalog. |
| Cursor setup | `com.xgen.mongot.cursor` | `mongot.search.create_and_register_cursor`, `mongot.cursor.*` | Creates cursor state, registers cursor/index mappings, and wires the batch producer. |
| Query build | `com.xgen.mongot.index.query`, `com.xgen.mongot.index.lucene` | `mongot.lucene.build_query` | Builds Lucene `Query` objects and records the Lucene query class/string. This constructs the query but does not execute it. |
| Text search execution | `com.xgen.mongot.index.lucene` | `mongot.lucene.collect_initial_top_docs` | Actual Lucene text/facet execution through `IndexSearcher.search(Query, CollectorManager)`. |
| Vector search execution | `com.xgen.mongot.index.lucene` | `mongot.lucene.vector_collect_candidates` | Actual vector candidate collection over the vector field, returning Lucene top-doc candidates before BSON conversion. |
| Result materialization | `com.xgen.mongot.index.lucene`, `com.xgen.mongot.index.query.pushdown.project` | `mongot.lucene.materialize_bson_documents` | Converts Lucene hits into BSON response documents. When Stored Source cannot satisfy the request, MongoT returns hit ids/scores and mongod performs the document fetch. |
| Response build | `com.xgen.mongot.server.command.search`, `com.xgen.mongot.cursor` | `mongot.search.prepare_response_document`, `mongot.search.encode_response_bson` | Builds and serializes the command response, cursor wrapper, and metadata variables. |

### Search Scenario Findings

These measurements came from client k6 runs with 25 VUs for search scenarios and detailed trace spans enabled:

| Scenario | Requests | Throughput | HTTP Median | MongoT Stream Median | Initial Command Median | Main Runtime Lesson |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Text Search Only | 166,253 | 554 req/s | 39.2 ms | 8.9 ms | 890 us | Stored-source text search spent most command time in BSON materialization, then parse/build/collect work. |
| Vector Search Only | 56,594 | 189 req/s | 124 ms | 10.0 ms | 2.8 ms | Vector candidate collection dominated the command span at roughly 1.9 ms median. |
| Both Vector And Text | 23,388 | 77.8 req/s | 268 ms | 37.8 ms | 2.2 ms | `$rankFusion` creates separate text and vector MongoT search streams; the vector sub-pipeline had a higher command median than the text sub-pipeline. |
| Document Fetch | 67,688 | 225 req/s | 87.0 ms | 11.9 ms | 863 us | Requesting fields outside Lucene Stored Source raised end-to-end client/MongoDB latency, while MongoT's initial command stayed sub-millisecond because the source-document fetch is mongod-owned. |

For combined text/vector search, the trace component split is especially important. The root operation name is the same for both sub-pipelines, but `mongot.search.index.name` separates them:

| Index | Stream Median | Command Median | Runtime Shape |
| --- | ---: | ---: | --- |
| `default` | 15.1 ms | 851 us | Text/facet pipeline with Lucene text hit collection and BSON materialization. |
| `vector_caption` | 24.2 ms | 4.3 ms | Vector pipeline with exact vector candidate collection as the dominant command cost. |

### Index Update Path

Insert/delete API calls are not children of a single client request trace inside MongoT. The client writes to MongoDB first; MongoT then observes those writes through steady-state change streams and updates Lucene asynchronously. Correlation is by run window, namespace/index generation attributes, `mongot.indexing.event.type`, and Lucene writer spans.

The package flow for updates is:

| Phase | Main Package(s) | Important Span(s) | What It Means |
| --- | --- | --- | --- |
| Change stream receive | `com.xgen.mongot.replication.mongodb.steadystate.changestream` | `mongot.indexing.change_stream_batch` | Receives a MongoDB change stream batch for an index generation. |
| Decode scheduling | `com.xgen.mongot.replication.mongodb.common` | `mongot.indexing.decode_batch` | Moves raw change-stream work into the decode scheduler while preserving trace context. |
| Event decode | `com.xgen.mongot.replication.mongodb.steadystate.changestream` | `mongot.indexing.decode_change_stream_events` | Converts raw change events into MongoT document events and records applicable/witnessed counts. |
| Index scheduling | `com.xgen.mongot.replication.mongodb.common` | `mongot.indexing.batch`, `mongot.indexing.document_event` | Runs indexing work and routes each insert/update/delete event to the index writer. |
| Document block build | `com.xgen.mongot.index.ingestion`, `com.xgen.mongot.index.lucene.writer` | `mongot.lucene.index_writer.build_document_block` | Converts MongoDB BSON into Lucene document blocks for indexed and stored fields. |
| Lucene update/delete | `com.xgen.mongot.index.lucene.writer` | `mongot.lucene.index_writer.update_documents`, `mongot.lucene.index_writer.delete_documents` | Calls Lucene `IndexWriter.updateDocuments` for inserts/updates or `IndexWriter.deleteDocuments` for deletes. |
| Commit and refresh | `com.xgen.mongot.index.lucene` | `mongot.lucene.index_writer.commit`, `mongot.lucene.maybe_refresh_searcher_manager` | Commits writer changes and refreshes the searcher manager so new segments become visible to search. |

The clean synthetic mutation run used one VU because the test API allocated ids before insertion, which raced under concurrent inserts. The final run produced 5,662 inserts and 5,662 deletes. Median indexing spans included change stream batch at about 300 us, indexing batch at about 146 us, document insert at about 168 us, Lucene update documents at about 54 us, Lucene delete documents at about 3 us, and periodic Lucene commit at about 20 ms for the few commit spans observed.

### What To Look At In Jaeger

For a search trace, start at the root stream span and then expand `mongot.search.command` or `mongot.vector_search.command`. The most useful attributes are:

| Attribute | Why It Matters |
| --- | --- |
| `mongot.operation.id` | Correlates the root command and child spans for one MongoT operation. |
| `db.namespace`, `mongodb.database.name`, `mongodb.collection.name` | Identifies the source namespace. |
| `mongot.search.index.name` | Separates text and vector sub-pipelines, especially in `$rankFusion` workloads. |
| `mongot.query.document` | Full input BSON, useful for understanding the exact query sent by the client. |
| `mongot.lucene.query.class`, `mongot.lucene.query` | Shows the Lucene query object that MongoT built from the BSON request. |
| `mongot.result.sample.count`, `mongot.result.sample.*` | Shows sampled BSON response documents after Lucene hits are materialized. |
| `mongot.indexing.event.type` | Distinguishes insert/update/delete work in asynchronous indexing traces. |

# Dependencies
This split is based on the actual `//:mongot_community` packaging rules plus Bazel dependency scopes. The first table covers dependencies that are bundled into the deployed mongot app or its deploy artifact set. The second table covers dependencies that are build-time, test-only, annotation-processor, or auxiliary tooling dependencies and are not bundled into the deployed app.

## Bundled In Deployed App

| Grouping | Dependency | Source File | Description | Website | Size |
|---|---|---|---|---|---:|
| `Cloud Integration` | `software.amazon.awssdk.crt:aws-crt` | `bazel/java/systems_deps.bzl` | AWS Common Runtime native bindings used by AWS SDK integrations. | [Website](https://github.com/awslabs/aws-crt-java) | 17.0 MB |
| `Cloud Integration` | `software.amazon.awssdk:s3` | `bazel/java/systems_deps.bzl` | AWS SDK v2 client for Amazon S3. | [Website](https://aws.amazon.com/sdk-for-java/) | 3.4 MB |
| `Cloud Integration` | `com.google.cloud:google-cloud-storage` | `bazel/java/systems_deps.bzl` | Google Cloud Storage client library. | [Website](https://github.com/googleapis/java-storage) | 1.4 MB |
| `Cloud Integration` | `software.amazon.awssdk:sdk-core` | `bazel/java/systems_deps.bzl` | AWS SDK v2 shared runtime core. | [Website](https://aws.amazon.com/sdk-for-java/) | 868 KB |
| `Cloud Integration` | `com.azure:azure-storage-blob` | `bazel/java/systems_deps.bzl` | Azure Blob Storage client library. | [Website](https://github.com/Azure/azure-sdk-for-java) | 843 KB |
| `Cloud Integration` | `software.amazon.awssdk:regions` | `bazel/java/systems_deps.bzl` | AWS SDK v2 region metadata and resolution support. | [Website](https://aws.amazon.com/sdk-for-java/) | 831 KB |
| `Cloud Integration` | `software.amazon.awssdk:secretsmanager` | `bazel/java/systems_deps.bzl` | AWS SDK v2 client for AWS Secrets Manager. | [Website](https://aws.amazon.com/sdk-for-java/) | 779 KB |
| `Cloud Integration` | `software.amazon.awssdk:sts` | `bazel/java/systems_deps.bzl` | AWS SDK v2 client for Security Token Service. | [Website](https://aws.amazon.com/sdk-for-java/) | 476 KB |
| `Cloud Integration` | `com.google.auth:google-auth-library-oauth2-http` | `bazel/java/systems_deps.bzl` | Google Auth HTTP/OAuth2 credential support. | [Website](https://github.com/googleapis/google-auth-library-java) | 316 KB |
| `Cloud Integration` | `com.azure:azure-identity` | `bazel/java/systems_deps.bzl` | Azure Identity client library for credentials and token acquisition. | [Website](https://github.com/Azure/azure-sdk-for-java) | 245 KB |
| `Cloud Integration` | `software.amazon.awssdk:s3-transfer-manager` | `bazel/java/systems_deps.bzl` | High-level transfer manager for S3 uploads and downloads. | [Website](https://aws.amazon.com/sdk-for-java/) | 224 KB |
| `Cloud Integration` | `software.amazon.awssdk:auth` | `bazel/java/systems_deps.bzl` | AWS SDK v2 authentication and credentials support. | [Website](https://aws.amazon.com/sdk-for-java/) | 218 KB |
| `Cloud Integration` | `com.google.cloud:google-cloud-nio` | `bazel/java/systems_deps.bzl` | NIO filesystem adapter for Google Cloud Storage. | [Website](https://github.com/googleapis/java-storage-nio) | 112 KB |
| `Cloud Integration` | `com.google.cloud:google-cloud-storage-control` | `bazel/java/systems_deps.bzl` | Google Cloud Storage Control API client. | [Website](https://github.com/googleapis/google-cloud-java) | 76 KB |
| `Cloud Integration` | `com.azure:azure-storage-blob-batch` | `bazel/java/systems_deps.bzl` | Azure Blob Storage batch operations client. | [Website](https://github.com/Azure/azure-sdk-for-java) | 35 KB |
| `Cloud Integration` | `com.google.auth:google-auth-library-credentials` | `bazel/java/systems_deps.bzl` | Google Auth core credential primitives. | [Website](https://github.com/googleapis/google-auth-library-java) | 8 KB |
| `Lucene` | `org.apache.lucene:lucene-analysis-nori` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 7.4 MB |
| `Lucene` | `org.apache.lucene:lucene-analysis-kuromoji` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 4.5 MB |
| `Lucene` | `org.apache.lucene:lucene-core` | `bazel/java/search_query_deps.bzl` | Apache Lucene core indexing and search engine library. | [Website](https://lucene.apache.org/) | 4.0 MB |
| `Lucene` | `org.apache.lucene:lucene-analysis-smartcn` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 3.4 MB |
| `Lucene` | `org.apache.lucene:lucene-analysis-common` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 1.7 MB |
| `Lucene` | `org.apache.lucene:lucene-analysis-stempel` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 516 KB |
| `Lucene` | `org.apache.lucene:lucene-queryparser` | `bazel/java/search_query_deps.bzl` | Lucene classic query parser and supporting parsers. | [Website](https://lucene.apache.org/) | 420 KB |
| `Lucene` | `org.apache.lucene:lucene-analysis-icu` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 92 KB |
| `Lucene` | `org.apache.lucene:lucene-analysis-morfologik` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 40 KB |
| `Lucene` | `org.apache.lucene:lucene-analysis-phonetic` | `bazel/java/search_query_deps.bzl` | Apache Lucene language analyzers and token filters. | [Website](https://lucene.apache.org/) | 36 KB |
| `Compression` | `com.github.luben:zstd-jni` | `bazel/java/deps.bzl` | JNI bindings for Zstandard compression. | [Website](https://github.com/luben/zstd-jni) | 6.5 MB |
| `Compression` | `org.xerial.snappy:snappy-java` | `bazel/java/deps.bzl` | Java bindings for Snappy compression. | [Website](https://github.com/xerial/snappy-java) | 2.3 MB |
| `Compression` | `org.apache.commons:commons-compress` | `bazel/java/deps.bzl` | Apache Commons archive and compression formats library. | [Website](https://commons.apache.org/proper/commons-compress/) | 1.1 MB |
| `Compression` | `commons-codec:commons-codec` | `bazel/java/deps.bzl` | Apache Commons codecs for hashes and binary/text encodings. | [Website](https://commons.apache.org/proper/commons-codec/) | 376 KB |
| `Utilities/Runtime` | `com.google.guava:guava` | `bazel/java/deps.bzl` | Google Guava core collections, caching, hashing, and utilities. | [Website](https://github.com/google/guava) | 2.9 MB |
| `Utilities/Runtime` | `org.apache.commons:commons-math3` | `bazel/java/deps.bzl` | Apache Commons mathematics and statistics library. | [Website](https://commons.apache.org/proper/commons-math/) | 2.1 MB |
| `Utilities/Runtime` | `org.apache.commons:commons-collections4` | `bazel/java/deps.bzl` | Apache Commons advanced collection types and utilities. | [Website](https://commons.apache.org/proper/commons-collections/) | 888 KB |
| `Utilities/Runtime` | `com.github.ben-manes.caffeine:caffeine` | `bazel/java/deps.bzl` | High-performance Java caching library. | [Website](https://github.com/ben-manes/caffeine) | 884 KB |
| `Utilities/Runtime` | `org.apache.commons:commons-lang3` | `bazel/java/deps.bzl` | Apache Commons general-purpose Java language utilities. | [Website](https://commons.apache.org/proper/commons-lang/) | 696 KB |
| `Utilities/Runtime` | `commons-io:commons-io` | `bazel/java/deps.bzl` | Apache Commons file and stream utilities. | [Website](https://commons.apache.org/proper/commons-io/) | 560 KB |
| `Utilities/Runtime` | `org.apache.commons:commons-text` | `bazel/java/deps.bzl` | Apache Commons string templating and text utilities. | [Website](https://commons.apache.org/proper/commons-text/) | 264 KB |
| `Utilities/Runtime` | `org.apache.commons:commons-rng-sampling` | `bazel/java/deps.bzl` | Apache Commons RNG modules for random-number APIs and sampling. | [Website](https://commons.apache.org/proper/commons-rng/) | 223 KB |
| `Utilities/Runtime` | `net.jodah:failsafe` | `bazel/java/deps.bzl` | Fault-tolerance library with retries, timeouts, and circuit breakers. | [Website](https://failsafe.dev/) | 108 KB |
| `Utilities/Runtime` | `com.squareup.okio:okio` | `bazel/java/deps.bzl` | Efficient I/O primitives from Square. | [Website](https://square.github.io/okio/) | 73 KB |
| `Utilities/Runtime` | `org.apache.commons:commons-rng-simple` | `bazel/java/deps.bzl` | Apache Commons RNG modules for random-number APIs and sampling. | [Website](https://commons.apache.org/proper/commons-rng/) | 52 KB |
| `Utilities/Runtime` | `org.apache.commons:commons-rng-client-api` | `bazel/java/deps.bzl` | Apache Commons RNG modules for random-number APIs and sampling. | [Website](https://commons.apache.org/proper/commons-rng/) | 25 KB |
| `Utilities/Runtime` | `com.google.auto.service:auto-service` | `bazel/java/deps.bzl` | Annotation processor for generating Java service provider configuration. | [Website](https://github.com/google/auto/tree/main/service) | 24 KB |
| `Utilities/Runtime` | `org.reactivestreams:reactive-streams` | `bazel/java/deps.bzl` | Reactive Streams standard interfaces for async stream processing. | [Website](https://www.reactive-streams.org/) | 11 KB |
| `Security/Crypto` | `org.bouncycastle:bc-fips` | `bazel/java/systems_deps.bzl` | Bouncy Castle FIPS-certified cryptography provider. | [Website](https://www.bouncycastle.org/download/bouncy-castle-java-fips/) | 3.9 MB |
| `Security/Crypto` | `org.bouncycastle:bctls-fips` | `bazel/java/systems_deps.bzl` | Bouncy Castle FIPS TLS/JSSE implementation. | [Website](https://www.bouncycastle.org/download/bouncy-castle-java-fips/) | 1.5 MB |
| `Security/Crypto` | `org.bouncycastle:bcpkix-fips` | `bazel/java/systems_deps.bzl` | Bouncy Castle FIPS PKIX/CMS/TSP APIs. | [Website](https://www.bouncycastle.org/download/bouncy-castle-java-fips/) | 1.0 MB |
| `Logging` | `org.apache.logging.log4j:log4j-core` | `bazel/java/deps.bzl` | Apache Log4j 2 core logging implementation. | [Website](https://logging.apache.org/log4j/2.x/) | 1.9 MB |
| `Logging` | `net.logstash.logback:logstash-logback-encoder` | `bazel/java/deps.bzl` | Structured JSON encoders for Logback. | [Website](https://github.com/logfellow/logstash-logback-encoder) | 440 KB |
| `Logging` | `ch.qos.logback:logback-classic` | `bazel/java/deps.bzl` | Logback SLF4J backend implementation. | [Website](https://logback.qos.ch/) | 312 KB |
| `Logging` | `com.google.flogger:flogger` | `bazel/java/deps.bzl` | Google Fluent Logger API and SLF4J backend. | [Website](https://github.com/google/flogger) | 212 KB |
| `Logging` | `org.slf4j:slf4j-api` | `bazel/java/deps.bzl` | SLF4J logging facade and JUL bridge. | [Website](https://www.slf4j.org/) | 80 KB |
| `Logging` | `com.google.flogger:flogger-slf4j-backend` | `bazel/java/deps.bzl` | Google Fluent Logger API and SLF4J backend. | [Website](https://github.com/google/flogger) | 16 KB |
| `Logging` | `org.slf4j:jul-to-slf4j` | `bazel/java/deps.bzl` | SLF4J logging facade and JUL bridge. | [Website](https://www.slf4j.org/) | 16 KB |
| `Monitoring` | `com.github.oshi:oshi-core` | `bazel/java/deps.bzl` | Cross-platform operating system and hardware information library. | [Website](https://github.com/oshi/oshi) | 1000 KB |
| `Monitoring` | `io.micrometer:micrometer-core` | `bazel/java/deps.bzl` | Micrometer core metrics facade and meter implementations. | [Website](https://micrometer.io/) | 856 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-exporter-otlp-common` | `bazel/java/deps.bzl` | Shared OTLP exporter support for OpenTelemetry. | [Website](https://opentelemetry.io/) | 228 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-api` | `bazel/java/deps.bzl` | OpenTelemetry tracing and metrics API. | [Website](https://opentelemetry.io/) | 164 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-exporter-common` | `bazel/java/deps.bzl` | Common exporter utilities for OpenTelemetry. | [Website](https://opentelemetry.io/) | 144 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-sdk-trace` | `bazel/java/deps.bzl` | OpenTelemetry trace SDK implementation. | [Website](https://opentelemetry.io/) | 140 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-exporter-otlp` | `bazel/java/deps.bzl` | OTLP exporter bundle for OpenTelemetry telemetry data. | [Website](https://opentelemetry.io/) | 81 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-sdk-common` | `bazel/java/deps.bzl` | Common OpenTelemetry SDK infrastructure. | [Website](https://opentelemetry.io/) | 80 KB |
| `Monitoring` | `io.micrometer:micrometer-registry-otlp` | `bazel/java/deps.bzl` | Micrometer registry for exporting metrics via OTLP. | [Website](https://micrometer.io/) | 72 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-context` | `bazel/java/deps.bzl` | OpenTelemetry context propagation library. | [Website](https://opentelemetry.io/) | 60 KB |
| `Monitoring` | `io.micrometer:micrometer-registry-prometheus` | `bazel/java/deps.bzl` | Micrometer registry for exposing metrics to Prometheus. | [Website](https://micrometer.io/) | 52 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-semconv` | `bazel/java/deps.bzl` | OpenTelemetry semantic conventions model. | [Website](https://opentelemetry.io/) | 48 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-exporter-logging-otlp` | `bazel/java/deps.bzl` | OpenTelemetry exporter that logs OTLP-formatted telemetry. | [Website](https://opentelemetry.io/) | 33 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-sdk` | `bazel/java/deps.bzl` | OpenTelemetry SDK aggregate module. | [Website](https://opentelemetry.io/) | 16 KB |
| `Monitoring` | `io.opentelemetry:opentelemetry-sdk-extension-resources` | `bazel/java/deps.bzl` | OpenTelemetry resource detection extensions. | [Website](https://opentelemetry.io/) | 16 KB |
| `Serialization` | `com.fasterxml.jackson.core:jackson-databind` | `bazel/java/deps.bzl` | Jackson data binding for mapping JSON/YAML and POJOs. | [Website](https://github.com/FasterXML/jackson-databind) | 1.6 MB |
| `Serialization` | `com.fasterxml.jackson.core:jackson-core` | `bazel/java/deps.bzl` | Jackson streaming JSON parser and generator core. | [Website](https://github.com/FasterXML/jackson-core) | 588 KB |
| `Serialization` | `org.yaml:snakeyaml` | `bazel/java/deps.bzl` | YAML parser and emitter for Java. | [Website](https://github.com/snakeyaml/snakeyaml) | 340 KB |
| `Serialization` | `com.fasterxml.jackson.datatype:jackson-datatype-jsr310` | `bazel/java/deps.bzl` | Jackson support for Java date/time types. | [Website](https://github.com/FasterXML/jackson-modules-java8) | 133 KB |
| `Serialization` | `com.fasterxml.jackson.core:jackson-annotations` | `bazel/java/deps.bzl` | Jackson annotations used to configure serialization and deserialization. | [Website](https://github.com/FasterXML/jackson-annotations) | 88 KB |
| `Serialization` | `com.fasterxml.jackson.dataformat:jackson-dataformat-yaml` | `bazel/java/deps.bzl` | Jackson YAML format module built on SnakeYAML. | [Website](https://github.com/FasterXML/jackson-dataformats-text) | 68 KB |
| `Serialization` | `com.fasterxml.jackson.datatype:jackson-datatype-jdk8` | `bazel/java/deps.bzl` | Jackson support for JDK 8 types such as Optional. | [Website](https://github.com/FasterXML/jackson-modules-java8) | 44 KB |
| `Serialization` | `com.fasterxml.jackson.module:jackson-module-parameter-names` | `bazel/java/deps.bzl` | Jackson module that infers constructor parameter names. | [Website](https://github.com/FasterXML/jackson-modules-java8) | 10 KB |
| `HTTP/Networking` | `org.apache.httpcomponents:httpclient` | `bazel/java/deps.bzl` | Apache HttpClient 4 synchronous HTTP client. | [Website](https://hc.apache.org/httpcomponents-client-4.5.x/) | 776 KB |
| `HTTP/Networking` | `org.apache.httpcomponents:httpcore` | `bazel/java/deps.bzl` | Apache HttpCore 4 low-level HTTP components. | [Website](https://hc.apache.org/httpcomponents-core-4.4.x/) | 332 KB |
| `CLI/Runtime` | `info.picocli:picocli` | `bazel/java/deps.bzl` | Command-line parser and ANSI CLI framework for Java. | [Website](https://picocli.info/) | 416 KB |
| `Data Access` | `org.mongodb:mongodb-driver-sync` | `bazel/java/systems_deps.bzl` | Official synchronous MongoDB Java driver. | [Website](https://www.mongodb.com/docs/drivers/java/sync/current/) | 160 KB |

## Not Bundled In Deployed App

| Dependency | Source File | Description | Website | Size |
|---|---|---|---|---:|
| `prometheus_macos_aarch64` | `bazel/tools/repos.bzl` | Prometheus monitoring server binary for macOS ARM64. | [Website](https://prometheus.io/) | 257.9 MB |
| `prometheus_macos_x86_64` | `bazel/tools/repos.bzl` | Prometheus monitoring server binary for macOS x86_64. | [Website](https://prometheus.io/) | 101.2 MB |
| `prometheus_linux_x86_64` | `bazel/tools/repos.bzl` | Prometheus monitoring server binary for Linux x86_64. | [Website](https://prometheus.io/) | 100.8 MB |
| `prometheus_linux_aarch64` | `bazel/tools/repos.bzl` | Prometheus monitoring server binary for Linux ARM64. | [Website](https://prometheus.io/) | 94.8 MB |
| `com_google_protobuf` | `bazel/proto/repos.bzl` | Protocol Buffers compiler/runtime repository used for code generation. | [Website](https://github.com/protocolbuffers/protobuf) | 92.2 MB |
| `io_grpc_grpc_java` | `bazel/java/repos.bzl` | gRPC Java framework and Bazel definitions. | [Website](https://github.com/grpc/grpc-java) | 20.7 MB |
| `io_bazel_stardoc` | `bazel/proto/repos.bzl` | Stardoc for generating documentation from Starlark. | [Website](https://github.com/bazelbuild/stardoc) | 20.6 MB |
| `bazel_gazelle` | `bazel/go/repos.bzl` | Gazelle for generating and updating Bazel build files, primarily for Go. | [Website](https://github.com/bazel-contrib/bazel-gazelle) | 13.6 MB |
| `io_bazel_rules_go` | `bazel/go/repos.bzl` | Bazel build rules for Go. | [Website](https://github.com/bazel-contrib/rules_go) | 11.0 MB |
| `distroless` | `bazel/docker/repos.bzl` | Minimal container base images and image definitions from Distroless. | [Website](https://github.com/GoogleContainerTools/distroless) | 9.3 MB |
| `rules_oci` | `bazel/docker/repos.bzl` | Bazel rules for building and publishing OCI container images. | [Website](https://github.com/bazel-contrib/rules_oci) | 9.1 MB |
| `rules_jvm_external` | `bazel/java/repos.bzl` | Bazel dependency resolver for Maven artifacts. | [Website](https://github.com/bazel-contrib/rules_jvm_external) | 8.1 MB |
| `rules_python` | `bazel/python/repos.bzl` | Bazel build rules for Python. | [Website](https://github.com/bazelbuild/rules_python) | 7.9 MB |
| `com.google.errorprone:error_prone_core` | `bazel/java/deps.bzl` | Error Prone compile-time bug checker implementation. | [Website](https://errorprone.info/) | 4.3 MB |
| `com.github.spotbugs:spotbugs` | `bazel/java/deps.bzl` | Static bug finder for Java bytecode. | [Website](https://spotbugs.github.io/) | 3.8 MB |
| `contrib_rules_jvm` | `bazel/java/repos.bzl` | Community Bazel JVM rules and utilities. | [Website](https://github.com/bazel-contrib/rules_jvm) | 3.3 MB |
| `com_github_bazelbuild_buildtools` | `bazel/buildifier/repos.bzl` | Buildifier and related tools for formatting and linting Bazel files. | [Website](https://github.com/bazelbuild/buildtools) | 2.4 MB |
| `com.puppycrawl.tools:checkstyle` | `bazel/java/deps.bzl` | Static style checker for Java source. | [Website](https://checkstyle.org/) | 2.0 MB |
| `rules_cc` | `bazel/common/repos.bzl` | Core Bazel rules for C and C++ builds. | [Website](https://github.com/bazelbuild/rules_cc) | 1.4 MB |
| `rules_poetry` | `WORKSPACE` | Python rules for Bazel with Poetry lockfile integration. | [Website](https://github.com/mongodb-forks/rules_poetry) | 980 KB |
| `com.google.guava:guava-testlib` | `bazel/java/deps.bzl` | Guava testing helpers for collection, concurrency, and API tests. | [Website](https://github.com/google/guava) | 920 KB |
| `rules_buf` | `bazel/proto/repos.bzl` | Bazel integration for Buf protobuf tooling. | [Website](https://github.com/bufbuild/rules_buf) | 728 KB |
| `com.google.errorprone:error_prone_check_api` | `bazel/java/deps.bzl` | Error Prone APIs for writing custom checks. | [Website](https://errorprone.info/) | 700 KB |
| `rules_java` | `bazel/java/repos.bzl` | Modern Bazel rules for Java builds. | [Website](https://github.com/bazelbuild/rules_java) | 684 KB |
| `org.openjdk.jmh:jmh-core` | `bazel/java/deps.bzl` | JMH microbenchmark harness runtime. | [Website](https://openjdk.org/projects/code-tools/jmh/) | 552 KB |
| `com.github.docker-java:docker-java-api` | `bazel/java/deps.bzl` | Docker Java client API interfaces and models. | [Website](https://github.com/docker-java/docker-java) | 492 KB |
| `rules_pkg` | `bazel/common/repos.bzl` | Bazel packaging rules for tar, zip, and OS packages. | [Website](https://github.com/bazelbuild/rules_pkg) | 464 KB |
| `com.uber.nullaway:nullaway` | `bazel/java/deps.bzl` | Nullness checker built on Error Prone. | [Website](https://github.com/uber/NullAway) | 428 KB |
| `toolchains_llvm` | `bazel/llvm/repos.bzl` | LLVM toolchain definitions for Bazel. | [Website](https://github.com/bazel-contrib/toolchains_llvm) | 416 KB |
| `rules_proto` | `bazel/proto/repos.bzl` | Bazel rules for Protocol Buffers. | [Website](https://github.com/bazelbuild/rules_proto) | 412 KB |
| `com.github.docker-java:docker-java-core` | `bazel/java/deps.bzl` | Core Docker Java client implementation. | [Website](https://github.com/docker-java/docker-java) | 384 KB |
| `junit:junit` | `bazel/java/deps.bzl` | JUnit 4 unit testing framework. | [Website](https://junit.org/junit4/) | 384 KB |
| `com_github_bazel_common` | `bazel/common/repos.bzl` | Shared Bazel macros and helpers from bazel-common. | [Website](https://github.com/google/bazel-common) | 355 KB |
| `rules_license` | `bazel/java/repos.bzl` | Bazel rules for collecting and checking license metadata. | [Website](https://github.com/bazelbuild/rules_license) | 304 KB |
| `com.google.truth:truth` | `bazel/java/deps.bzl` | Google Truth fluent assertion library. | [Website](https://truth.dev/) | 280 KB |
| `rules_shell` | `bazel/shell/repos.bzl` | Bazel rules for shell scripts and sh tests. | [Website](https://github.com/bazelbuild/rules_shell) | 240 KB |
| `apple_rules_lint` | `bazel/java/repos.bzl` | Apple-oriented Bazel linting helpers used here for repo tooling. | [Website](https://github.com/bazelbuild/apple_rules_lint) | 172 KB |
| `com.pholser:junit-quickcheck-core` | `bazel/java/deps.bzl` | Property-based testing support for JUnit. | [Website](https://pholser.github.io/junit-quickcheck/site/1.0/) | 148 KB |
| `bazel_rules_mongo` | `WORKSPACE` | MongoDB-specific Bazel macros and repository setup used by this project. | [Website](https://github.com/mongodb-labs/bazel_rules_mongo) | 128 KB |
| `com.pholser:junit-quickcheck-generators` | `bazel/java/deps.bzl` | Generator library for junit-quickcheck property tests. | [Website](https://pholser.github.io/junit-quickcheck/site/1.0/) | 124 KB |
| `platforms` | `bazel/common/repos.bzl` | Canonical Bazel platform constraint definitions. | [Website](https://github.com/bazelbuild/platforms) | 56 KB |
| `com.googlecode.junit-toolbox:junit-toolbox` | `bazel/java/deps.bzl` | JUnit helpers for suites, parallel runs, and categories. | [Website](https://github.com/MichaelTamm/junit-toolbox) | 52 KB |
| `com.github.docker-java:docker-java-transport` | `bazel/java/deps.bzl` | Transport abstraction used by docker-java. | [Website](https://github.com/docker-java/docker-java) | 48 KB |
| `org.jetbrains:annotations` | `bazel/java/deps.bzl` | JetBrains nullability and tooling annotations. | [Website](https://github.com/JetBrains/java-annotations) | 40 KB |
| `org.openjdk.jmh:jmh-generator-annprocess` | `bazel/java/deps.bzl` | JMH annotation processor for generating benchmark scaffolding. | [Website](https://openjdk.org/projects/code-tools/jmh/) | 40 KB |
| `com.googlecode.json-simple:json-simple` | `bazel/java/deps.bzl` | Lightweight JSON parsing and writing library. | [Website](https://code.google.com/archive/p/json-simple/) | 32 KB |
| `com.github.docker-java:docker-java-transport-httpclient5` | `bazel/java/deps.bzl` | Apache HttpClient 5 transport for docker-java. | [Website](https://github.com/docker-java/docker-java) | 28 KB |
| `com.google.errorprone:error_prone_annotations` | `bazel/java/deps.bzl` | Error Prone annotations used by static analysis and nullness tooling. | [Website](https://errorprone.info/) | 28 KB |
| `org.hamcrest:hamcrest-core` | `bazel/java/deps.bzl` | Hamcrest matcher library for expressive tests. | [Website](https://hamcrest.org/JavaHamcrest/) | 12 KB |
| `org.hamcrest:hamcrest-library` | `bazel/java/deps.bzl` | Hamcrest matcher library for expressive tests. | [Website](https://hamcrest.org/JavaHamcrest/) | 12 KB |
| `org.mockito:mockito-subclass` | `bazel/java/deps.bzl` | Mockito subclass-based mock maker support. | [Website](https://site.mockito.org/) | 12 KB |
