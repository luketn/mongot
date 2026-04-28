# Search and Vector Search Tracing

This document describes the current tracing surface for MongoT search commands and the detailed
span hierarchy added for local diagnosis of `$search` and `$vectorSearch` requests.

## How Traces Reach Grafana

Local tracing is started by `local-monitoring.sh`, which runs Jaeger with OTLP ports `4317` and
`4318` exposed. Grafana is provisioned with a Jaeger data source named `jaeger`, so traces exported
to Jaeger can be opened from Grafana.

Grafana is also provisioned with Prometheus panels that use the MongoT metrics endpoint. The
headline search latency panels use `mongot_command_searchCommandStreamLatency_seconds`, a
server-side gRPC stream timer recorded from stream creation until MongoT sends its half-close. This
is closer to the Jaeger end-to-end server span than `searchCommandTotalLatency`, which is only the
individual command response path.

MongoT can export traces in two ways:

- The OpenTelemetry Java agent, configured in `LOCAL-RUN.md` and `INTELLIJ-QUICKSTART.md`, exports
  auto-instrumented spans to `http://127.0.0.1:4318`.
- The in-repo manual tracer in `com.xgen.mongot.trace.Tracing` creates an SDK tracer named
  `mongot-tracing`. When `otel.traces.exporter=otlp` is configured, it exports sampled manual spans
  through OTLP/HTTP to the same endpoint as the Java agent. Otherwise it falls back to OTLP JSON log
  lines through `Slf4jExporter`. It does not register itself as the global OpenTelemetry instance,
  so it can run cleanly alongside the OpenTelemetry Java agent.

The in-repo tracer uses `ToggleSampler`. Its default sample rate is `0`, so manual spans are
normally dropped unless a root span sets the `toggleTrace` attribute to `true`.

## Current Manual Trace Coverage

Before the detailed tracing changes, the relevant manual spans were sparse and mostly unavailable in
normal search traces:

| Span name | Location | Sampling | Useful fields |
| --- | --- | --- | --- |
| `SearchCommand.run` | `SearchCommand.run()` | Explicit `Tracing.TOGGLE_OFF` | None |
| `SearchCommand.getIntermediateBatch` | Intermediate `$search` protocol path | Inherits parent sampling | None |
| `Query.fromBson` | `$search` query parse | Inherits parent sampling | None |
| `VectorSearchCommand.getSearchResults` | `VectorSearchCommand.getSearchResults()` | Explicit `Tracing.TOGGLE_OFF` | None |
| `VectorSearchQuery.fromBson` | Test-only vector parse helper | Inherits parent sampling | None |
| `LuceneSearchBatchProducer.getSearchResults` | Text result retrieval and serialization | Inherits parent sampling | Result materialization timing through explain, but few span attributes |
| `VectorIndexReader.getBsonArray` | Vector result BSON serialization | Inherits parent sampling | `num vector results`, `time` |
| `VectorIndexReader.getResults` | Vector result materialization | Inherits parent sampling | Existing code path span, limited attributes |

In practice, this meant the main `$search` and `$vectorSearch` command spans were created with
sampling forced off. Any child spans below them inherited the dropped sampling decision, so Grafana
usually showed only Java-agent spans, not the application-level MongoT/Lucene work.

## Identification and Correlation

With detailed tracing enabled, command root spans are named:

- `mongot.search.command`
- `mongot.vector_search.command`

Each root span includes:

- `service.name=mongot`
- `db.system=mongodb`
- `db.operation.name`
- `db.namespace`
- `mongodb.database.name`
- `mongodb.collection.name`
- `mongodb.collection.uuid`
- `mongot.command.name`
- `mongot.operation.id`

`mongot.operation.id` is set to the OpenTelemetry trace ID for the MongoT command root span. Use it
as the MongoT-side operation identifier in Grafana. It is also visible as the trace ID in Jaeger.

Important caveat: I did not find trace-context extraction from the MongoDB wire protocol or
search-envoy metadata in this code path. That means a Java app's MongoDB driver span will not
automatically share a trace ID with the MongoT command span unless another component propagates the
context. Correlate by timestamp, namespace, index name, and `mongot.operation.id` on the MongoT
side. If the client includes a query `comment` in the future, adding it as a span attribute would be
the cleanest client-provided correlation key.

## Detailed Tracing Toggle

Detailed diagnostic spans are controlled by:

```bash
DETAILED_TRACE_SPANS=true
```

When the variable is absent or false:

- command root spans set `toggleTrace=false`, preserving the previous default behavior;
- new detailed spans return a no-op guard and are not allocated/exported.

When it is true:

- command root spans set `toggleTrace=true`;
- child spans inherit the sampled parent decision;
- detailed MongoT/Lucene spans appear below the command root.
- detailed span durations are also emitted as
  `mongot_trace_detailed_span_duration_seconds{span="..."}` so Grafana can chart phase timings.

## New Span Hierarchy

Typical `$search` trace:

```text
mongot.search.command
  mongot.search.prepare_query_context
  mongot.search.parse_query_bson
    Query.fromBson
  mongot.search.lookup_index_catalog
  mongot.search.parse_cursor_options
  mongot.search.validate_query_options
  mongot.search.prepare_response_mode
  mongot.search.build_first_batch
    mongot.search.create_and_register_cursor
      mongot.cursor.select_index_manager
      mongot.cursor.create_cursor_state
        mongot.cursor.choose_batch_size
        mongot.cursor.open_batch_producer
          mongot.lucene.prepare_search_reader_query
            mongot.lucene.open_index_searcher
            mongot.lucene.build_query
            mongot.lucene.prepare_highlights
            mongot.lucene.prepare_score_details
            mongot.lucene.prepare_pagination_sort_context
            mongot.lucene.build_sort
            mongot.lucene.create_search_manager
            mongot.lucene.collect_initial_top_docs
            mongot.lucene.create_result_producer
        mongot.cursor.instantiate_cursor
      mongot.cursor.register_active_cursor
      mongot.cursor.register_index_mapping
    mongot.search.load_first_cursor_batch
      mongot.cursor.get_next_batch.initial
        mongot.cursor.advance_batch_producer
        mongot.cursor.read_batch_results
          mongot.lucene.materialize_bson_documents
    mongot.search.prepare_response_document
    mongot.search.encode_response_bson
```

Typical `$vectorSearch` trace:

```text
mongot.vector_search.command
  mongot.vector_search.parse_query
  mongot.vector_search.validate_query
  mongot.vector_search.resolve_index
  mongot.vector_search.validate_stored_source
  VectorSearchCommand.getSearchResults
    mongot.vector_search.materialize_query
      mongot.vector_search.embedding_service  (auto-embedding only)
    mongot.vector_search.reader_query         (exhausted-batch path)
      mongot.lucene.vector_index_reader.query
        mongot.lucene.vector_index_reader.query_results
          mongot.lucene.create_vector_query
          mongot.lucene.vector_initial_top_docs
            mongot.lucene.vector_collect_candidates
            mongot.lucene.vector_quantized_rescore      (if quantized)
            mongot.lucene.vector_nested_avg_rescore     (if nested)
          mongot.lucene.vector_materialize_results
        VectorIndexReader.getBsonArray
    mongot.vector_search.create_cursor        (stored-source cursor path)
    mongot.vector_search.cursor_next_batch
      mongot.lucene.vector_serialize_batch
    mongot.vector_search.serialize_batch
```

Vector search over a search index uses `mongot.lucene.prepare_search_reader_query` and
`mongot.lucene.create_vector_query` before the vector top-doc spans.

## Attribute Fields Added

Search command and parsing spans:

- `mongot.search.index.name`
- `mongot.search.query.type`
- `mongot.query.document` on `$search` roots
- `mongot.search.allow_10k_bucket_limit`
- `mongot.search.index.found`
- `mongot.search.index.partitions`
- `mongot.search.populate_cursor_result`

Vector command spans:

- `mongot.vector.path`
- `mongot.vector.search.type`
- `mongot.vector.limit`
- `mongot.vector.return_stored_source`
- `mongot.vector.has_filter`
- `mongot.vector.has_query_text`
- `mongot.vector.has_query_vector`
- `mongot.vector.query_vector.type`
- `mongot.embedding.model`
- `mongot.embedding.result.count`

Lucene spans:

- `mongot.lucene.index.partition`
- `mongot.lucene.query.class`
- `mongot.lucene.highlighter.present`
- `mongot.lucene.score_details.present`
- `mongot.lucene.sort.present`
- `mongot.lucene.batch_size`
- `mongot.lucene.top_docs.count`
- `mongot.lucene.total_hits`
- `mongot.lucene.exhausted`
- `mongot.lucene.candidate.count`
- `mongot.lucene.rescored.count`

Batch/cursor spans:

- `mongot.cursor.id`
- `mongot.batch.exhausted`
- `mongot.batch.result.count`
- `mongot.batch.data_size.bytes`
- `mongot.batch.bson.field_count`
- `mongot.vector.total_prefetched_results`

## Running a Local Test

Start the local stack:

```bash
./local-mongod.sh
./local-monitoring.sh
```

Run MongoT with the Java agent and detailed spans enabled:

```bash
cd /Users/luketn/code/personal/mongot
bazel build //src/main/java/com/xgen/mongot/community:MongotCommunity

JAVABIN=$(/usr/libexec/java_home -v 21)/bin/java \
DETAILED_TRACE_SPANS=true ./bazel-bin/src/main/java/com/xgen/mongot/community/MongotCommunity \
  --jvm_flag="-javaagent:$(pwd)/otel-agent/opentelemetry-javaagent.jar" \
  --jvm_flag="-Dotel.traces.exporter=otlp" \
  --jvm_flag="-Dotel.exporter.otlp.endpoint=http://127.0.0.1:4318" \
  --jvm_flag="-Dotel.service.name=mongot" \
  --jvm_flag="-Dotel.metrics.exporter=none" \
  --jvm_flag="-Dotel.logs.exporter=none" \
  --config mongot-dev.yml --internalListAllIndexesForTesting
```

Use the Bazel launcher target above rather than `bazel-bin/mongot_community_deploy.jar`; the launcher
keeps the BouncyCastle FIPS jars in the layout expected by their checksum validation.

Run the Java client/load test from:

```bash
/Users/luketn/code/personal/atlas-search-coco
```

In Grafana, open the Jaeger data source or the trace panels and filter for
`service.name=mongot`. Look for root spans named `mongot.search.command` or
`mongot.vector_search.command`, then use `mongot.operation.id` or the trace ID to inspect the full
MongoT-side request breakdown.

The `MongoT - Metrics & Traces` dashboard has two search views:

- `Search Stream Latency (...)` uses the stream-level timer for the more representative
  server-side end-to-end duration.
- `Search Stream Breakdown (mean)` stacks non-overlapping text-search phases and separates Lucene
  query-build, hit collection, cursor, response-assembly, and stream-lifecycle work. Residual
  buckets are labelled as bookkeeping for the component that owns them rather than as generic
  "other" work.
- `Search Command Phase Breakdown (mean, us)` excludes the stream lifecycle and uses microseconds
  so the sub-millisecond command and Lucene phases remain visible.
