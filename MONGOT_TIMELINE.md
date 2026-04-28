# MongoT Runtime Timeline

This timeline is derived from the package tour plus the actual bootstrap, config, lifecycle, replication, cursor, and server code paths.

Arrows are package-level flows, backed by classes such as:

- `community`: `MongotCommunity`, `CommunityMongotBootstrapper`
- `config`: `DefaultConfigManager`, `CommunityConfigUpdater`, `PeriodicConfigMonitor`
- `lifecycle`: `DefaultLifecycleManager`, `IndexLifecycleManager`
- `replication`: `MongoDbReplicationManager`, `ReplicationIndexManager`, `InitialSyncQueue`, `ChangeStreamManager`
- `server`: `GrpcStreamingServer`, `ServerCallHandler`, `SearchCommand`, `VectorSearchCommand`
- `cursor`: `MongotCursorManagerImpl`, `IndexCursorManagerImpl`, `CursorFactory`
- `index`: initialized index creation plus Lucene-backed readers/writers
- `catalogservice`: authoritative catalog and metadata service

The startup, indexing, steady-state, and shutdown flows below are grounded in code. The later Atlas Search and Vector Search load stages are modeled from the serving paths so their different dependencies stay visible in the graph, not from a production trace.

## 1. Bootstrap, Control Plane, and Service Initialization

This diagram is entirely inside the `mongot` executable. MongoT starts by letting `community` hand control to `config`, which fans out into logging, metrics, monitors, metadata, cursors, and serving infrastructure.

```mermaid
sequenceDiagram
    autonumber
    participant community as community
    participant logging as logging
    participant metrics as metrics
    participant monitor as monitor
    participant catalogservice as catalogservice
    participant embedding as embedding
    participant cursor as cursor
    participant config as config
    participant lifecycle as lifecycle
    participant server as server

    community->>logging: bridge JUL, install crash handler, install FIPS provider
    community->>config: CommunityMongotBootstrapper.bootstrap(...)
    config->>logging: configure root level and optional file appender
    config->>metrics: create meter registry, FTDC, Prometheus, system metrics
    config->>cursor: MongotCursorManagerImpl.fromConfig(...)
    config->>monitor: start disk monitor and replication state monitor
    config->>catalogservice: initialize metadata client and metadata service
    config->>embedding: initialize embedding service supplier
    config->>config: DefaultConfigManager.initialize(...)
    config->>server: assemble gRPC servers, executors, and health manager

    par config monitor thread
        config->>config: PeriodicConfigMonitor.start()
        loop every polling period
            config->>catalogservice: AuthoritativeIndexCatalog.listIndexes()
            catalogservice-->>config: desired index definitions
            config->>config: update(...) then updateCycle(...)
        end
    and metadata updater thread
        config->>catalogservice: CommunityMetadataUpdater.start()
        loop every 30s
            catalogservice-->>catalogservice: write serverState + indexStats
        end
    and request-serving threads
        config->>server: serverLifecycles.start.run()
        config->>server: healthCheckServer.start()
        server-->>server: accept gRPC streams and commands
    end
```

## 2. Initialize, Initial Sync, and Transition to Steady State

This diagram is entirely inside the `mongot` executable. Once the process is up, lifecycle and replication cooperate to initialize indexes, establish durable resume state, and move each generation into steady service.

```mermaid
sequenceDiagram
    autonumber
    participant config as config
    participant lifecycle as lifecycle
    participant index as index
    participant replication as replication
    participant initq as "replication.initialsync"
    participant steady as "replication.steadystate"
    participant monitor as monitor

    config->>lifecycle: startLifecycle(configState)
    loop for each live or staged index generation
        lifecycle->>lifecycle: add(indexGeneration)
        lifecycle->>index: initialize() and getInitializedIndex(...)
        index-->>lifecycle: initialized index instance
        lifecycle->>replication: startReplication() then add(indexGeneration)
        replication->>replication: ReplicationIndexManager.init()

        alt fresh index or missing durable resume state
            replication->>initq: enqueueInitialSync(IndexStatus.initialSync())
            par initial sync dispatcher
                initq->>initq: dispatcher pulls queued work and starts InitialSyncManager
            and collection scan worker
                initq->>index: CollectionScanner batches documents into IndexingWorkScheduler
            and change stream buffer worker
                initq->>initq: ChangeStreamBufferManager buffers concurrent writes
            end
            initq->>index: ChangeStreamBufferApplier replays buffered events
            initq->>index: PeriodicIndexCommitter persists resume data
            initq-->>replication: final resume token / minValid optime
            replication->>steady: ChangeStreamManager.add(...)
        else restart with prior state on disk
            replication->>replication: determineInitAction(...)
            alt initial sync resume info present
                replication->>initq: enqueueInitialSyncResume(...)
            else steady-state resume info present
                replication->>steady: resumeSteadyState(...)
            end
        end

        par steady-state replication thread
            steady->>steady: dispatcher polls change stream batches round-robin
            steady->>index: IndexingWorkScheduler submits ordered document batches
            index-->>steady: DocumentIndexer updates commit user data
            steady->>index: PeriodicIndexCommitter writes resume token + optime
        and protection loop
            monitor->>config: disk/replication state changes may pause or restart replication
        end
    end
```

## 3. Stable Serving: Atlas Search, Vector Search, and Cursor Continuations

This diagram is entirely inside the `mongot` executable. Stable serving splits into two distinct query paths: Atlas Search uses the server-cursor-index chain, while Vector Search adds the embedding service before the index reader.

```mermaid
sequenceDiagram
    autonumber
    participant server as server
    participant cursor as cursor
    participant embedding as embedding
    participant index as index
    participant replication as replication
    participant config as config
    participant catalogservice as catalogservice

    par Atlas Search request stream
        server->>server: ServerCallHandler.parseCommand(...)
        server->>server: CommandRegistry lookup + BulkheadCommandExecutor.execute(...)
        server->>cursor: SearchCommand.run() then newCursor(...)
        cursor->>index: CursorFactory.createCursor() then index.getReader().query(...)
        index-->>cursor: search batch producer + meta results
        cursor-->>server: first batch and cursor id
        loop getMore
            server->>cursor: GetMoreCommand.run() then getNextBatch(...)
            cursor->>index: produce next page from query cursor
            index-->>cursor: next batch
            cursor-->>server: page of results
        end
    and Vector Search request stream
        server->>server: ServerCallHandler.parseCommand(...)
        server->>server: BulkheadCommandExecutor.execute(...)
        server->>embedding: VectorSearchCommand.maybeEmbed(...)
        embedding-->>server: embedded query vector when text input is used
        server->>index: index.asVectorIndex().getReader().query(...)
        index-->>server: vector result batch
    and steady-state indexing stream
        replication->>index: change stream batches continue to update indexes
        index-->>replication: commit user data and fresh resume tokens
    and control-plane maintenance
        config->>catalogservice: poll desired definitions
        catalogservice-->>config: updated definitions and metadata
    end
```

## 4. Shutdown

This diagram is entirely inside the `mongot` executable. Shutdown unwinds the same stack in reverse order so serving, telemetry, config, lifecycle, and replication all stop cleanly without orphaned work.

```mermaid
sequenceDiagram
    autonumber
    participant community as community
    participant server as server
    participant metrics as metrics
    participant config as config
    participant lifecycle as lifecycle
    participant replication as replication
    participant catalogservice as catalogservice
    participant logging as logging

    community->>server: health manager enters terminal state
    community->>server: stop gRPC servers and executors
    community->>metrics: stop system metrics, Prometheus, and FTDC
    community->>catalogservice: stop metadata updater
    community->>config: close()
    config->>lifecycle: shutdown()
    lifecycle->>replication: currentReplicationManager.shutdown()
    replication->>replication: stop per-index managers, initial sync queue, steady state manager, synonym manager
    replication-->>lifecycle: executors, clients, sessions, schedulers closed
    config-->>community: indexes, cursors, factories, and materialized views closed
    community->>catalogservice: close metadata service
    community->>logging: shutdown()
```

## 5. Detailed Trace and Load Test Views

The diagrams below add the runtime detail learned from client load-test runs recorded with `DETAILED_TRACE_SPANS=true`. The broad package flows above show ownership and lifecycle; these diagrams show the shape of individual traced requests and update batches.

### 5.1 Text Search Trace

Text search uses the command-stream path, then creates a cursor backed by a Lucene search batch producer. In the measured text-only run, the median root stream span was `8.9 ms` and the median initial `mongot.search.command` span was `890 us`.

On the mongod side, `$search` is parsed by [`DocumentSourceSearch::createFromBson`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L128), rewritten by [`DocumentSourceSearch::desugar`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_search.cpp#L166) (that is, rewritten from the public stage syntax into lower-level internal execution stages), and then executed by establishing a MongoT cursor through [`search_helpers::establishSearchCursors`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L438) and [`mongot_cursor::establishCursorsForSearchStage`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L225). The command body is built in [`mongot_cursor::getRemoteCommandRequestForSearchQuery`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L49), [`mongot_cursor::getRemoteCommandRequest`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) targets the configured MongoT address, and [`mongot_cursor::establishCursors`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L197) creates the `TaskExecutorCursor` that invokes MongoT.

```mermaid
sequenceDiagram
    autonumber
    participant app as "java: client app"
    participant driver as "java: MongoDB Java driver"
    participant mongod as "mongod: aggregation executor"
    participant server as "mongot: gRPC command stream"
    participant command as "mongot: SearchCommand"
    participant query as "mongot: index.query"
    participant catalog as "mongot: catalog/index"
    participant cursor as "mongot: cursor"
    participant lucene as "mongot: index.lucene"

    app->>driver: aggregate with $search text/facet
    driver->>mongod: aggregate command
    mongod->>server: gRPC command stream search
    activate server
    server->>command: mongot.search.command
    activate command
    command->>query: parse BSON - mongot.search.parse_query_bson
    query-->>command: SearchQuery collector/text model
    command->>catalog: lookup index catalog - mongot.search.lookup_index_catalog
    catalog-->>command: default search index
    command->>cursor: create and register cursor
    activate cursor
    cursor->>lucene: prepare search reader query
    activate lucene
    lucene->>lucene: build Lucene query - mongot.lucene.build_query
    lucene->>lucene: Lucene collect hits - IndexSearcher.search(Query, CollectorManager)
    lucene->>lucene: materialize BSON documents - stored fields / stored source
    lucene-->>cursor: first batch producer + meta results
    deactivate lucene
    cursor-->>command: first batch and cursor id
    deactivate cursor
    command-->>server: response document + encoded BSON
    deactivate command
    server-->>mongod: first search response batch
    loop cursor continuation inside same stream
        driver->>mongod: getMore on MongoDB cursor
        mongod->>server: getMore / stream consumption
        server->>cursor: getNextBatch
        cursor->>lucene: advance batch producer
        lucene-->>cursor: next BSON batch
        cursor-->>server: page of results
        server-->>mongod: search response batch
        mongod-->>driver: aggregate cursor batch
    end
    deactivate server
    driver-->>app: search response
```

### 5.2 Vector Search Trace

Vector search follows the same command-stream skeleton but the dominant command-phase work is vector candidate collection over the `vector_caption` index and `captionEmbedding` vector field. In the measured vector-only run, the median root stream span was `10.0 ms`, the command span was `2.8 ms`, and vector candidate collection was about `1.9 ms` median.

On the mongod side, `$vectorSearch` is parsed by [`DocumentSourceVectorSearch::createFromBson`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_vector_search.cpp#L177). The executable stage is created by [`documentSourceVectorSearchToStageFn`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L39), and [`VectorSearchStage::doGetNext`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/exec/agg/search/vector_search_stage.cpp#L138) establishes the MongoT cursor through [`search_helpers::establishVectorSearchCursor`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L74). The vector command body is built in [`getRemoteCommandRequestForVectorSearchQuery`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/vector_search_helper.cpp#L41), which delegates to [`mongot_cursor::getRemoteCommandRequest`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor.cpp#L187) to target the configured MongoT address.

```mermaid
sequenceDiagram
    autonumber
    participant app as "java: client app"
    participant embedding as "external: embedding provider / LM Studio"
    participant driver as "java: MongoDB Java driver"
    participant mongod as "mongod: aggregation executor"
    participant server as "mongot: gRPC command stream"
    participant command as "mongot: VectorSearchCommand"
    participant query as "mongot: index.query"
    participant cursor as "mongot: cursor"
    participant lucene as "mongot: index.lucene vector reader"

    app->>embedding: build query embedding
    embedding-->>app: 768-dimension query vector
    app->>driver: aggregate with vector search
    driver->>mongod: aggregate command
    mongod->>server: gRPC vectorSearch command stream
    activate server
    server->>command: mongot.vector_search.command
    activate command
    command->>query: parse BSON vectorSearch
    query-->>command: VectorSearchOperator / materialized criteria
    command->>cursor: create cursor for vector_caption
    activate cursor
    cursor->>lucene: prepare vector reader query
    activate lucene
    lucene->>lucene: build Lucene vector query - mongot.lucene.build_query
    lucene->>lucene: Lucene vector candidates - mongot.lucene.vector_collect_candidates
    lucene->>lucene: materialize BSON vector results
    lucene-->>cursor: vector TopDocs + BSON docs
    deactivate lucene
    cursor-->>command: first vector batch
    deactivate cursor
    command-->>server: response document
    deactivate command
    server-->>mongod: vector search response batch
    deactivate server
    mongod-->>driver: aggregate cursor batch
    driver-->>app: vector search response
```

### 5.3 Combined Text And Vector With `$rankFusion`

The combined workload is not one monolithic MongoT search. The client builds a `$rankFusion` aggregation, and MongoT sees separate search streams for the text and vector sub-pipelines. The two sub-pipelines share the same root operation name, so `mongot.search.index.name` is the attribute that separates `default` from `vector_caption` traces.

On the mongod side, `$rankFusion` is parsed by [`DocumentSourceRankFusion::createFromBson`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/document_source_rank_fusion.cpp#L270). [`parseAndValidateRankedSelectionPipelines`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/document_source_rank_fusion.cpp#L216) parses each branch, [`HybridSearchPipelineBuilder::constructDesugaredOutput`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/hybrid_search_pipeline_builder.cpp#L112) expands them into ordinary pipeline stages, and [`RankFusionPipelineBuilder::buildScoreAndMergeStages`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/rank_fusion_pipeline_builder.cpp#L425) performs the final fusion after the individual search branches return.

```mermaid
sequenceDiagram
    autonumber
    participant app as "java: client app"
    participant driver as "java: MongoDB Java driver"
    participant mongod as "mongod: aggregation executor"
    participant text as "mongot: text stream - index=default"
    participant vector as "mongot: vector stream - index=vector_caption"
    participant luceneText as "mongot: Lucene text search"
    participant luceneVector as "mongot: Lucene vector search"

    app->>driver: aggregate with $rankFusion
    driver->>mongod: fused text + vector pipelines
    par text sub-pipeline
        mongod->>text: $search text pipeline
        activate text
        text->>luceneText: build query + collect text hits
        luceneText-->>text: TopDocs + materialized BSON
        text-->>mongod: text ranked results
        deactivate text
    and vector sub-pipeline
        mongod->>vector: $search vectorSearch pipeline
        activate vector
        vector->>luceneVector: build vector query + collect candidates
        luceneVector-->>vector: vector TopDocs + materialized BSON
        vector-->>mongod: vector ranked results
        deactivate vector
    end
    mongod->>mongod: rank fusion and final projection
    mongod-->>driver: fused result set
    driver-->>app: combined search response
```

Measured component split from the combined run:

| Sub-pipeline | Index | Stream Median | Command Median | Runtime Signal |
| --- | --- | ---: | ---: | --- |
| Text | `default` | 15.1 ms | 851 us | Text/facet query, Lucene text hit collection, BSON materialization. |
| Vector | `vector_caption` | 24.2 ms | 4.3 ms | Exact vector candidate collection dominates command time. |

### 5.4 Document Fetch For Missing Stored Source Fields

This workload uses text search, but the client query asks for fields that are not returned from Lucene Stored Source. This is not the Java MongoDB driver receiving a batch of `_id`s from MongoT and issuing a second client-side query. The Java driver builds one `aggregate` command and sends it to `mongod`; any client cursor continuation is still client-to-mongod.

The `_id` handoff is server-side. When the requested response cannot be satisfied from Stored Source, MongoT's Lucene projection path uses `IdLookupFactory`: it returns each hit's root `_id` and leaves document materialization to `mongod`. The mongod-side search pipeline then uses those ids to fetch the matched MongoDB documents and apply the final aggregation projection before returning cursor batches to the driver. MongoT itself is not opening a separate query back to mongod for the missing fields.

So "Document Fetch" means the client-visible projection requires fields outside the stored-source-only response shape used by the lighter workload. That forces full-document materialization inside MongoDB's aggregation/search execution path. The measured effect was mostly outside the initial MongoT command span: HTTP and app-reported MongoDB time increased, while the median `mongot.search.command` stayed around `863 us`.

The mongod decision point is [`search_helpers::promoteStoredSourceOrAddIdLookup`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/search_helper.cpp#L709): stored source is promoted directly when available, otherwise mongod inserts [`DocumentSourceInternalSearchIdLookUp`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/document_source_internal_search_id_lookup.cpp#L65). Document fetch does not inherently mean MongoT `getMore`; mongod can request a large enough MongoT batch and then send `killCursors`. MongoT `getMore` remains a real cursor-continuation path through [`MongotTaskExecutorCursorGetMoreStrategy::createGetMoreRequest`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/query/search/mongot_cursor_getmore_strategy.cpp#L60), and it is used by workloads that need continued MongoT cursor batches.

```mermaid
sequenceDiagram
    autonumber
    participant app as "java: client app"
    participant driver as "java: MongoDB Java driver"
    participant mongod as "mongod: aggregation executor"
    participant server as "mongot: gRPC command stream"
    participant command as "mongot: SearchCommand"
    participant lucene as "mongot: index.lucene"
    participant idLookup as "mongod: id lookup / materialization"
    participant appProject as "java: client projection"

    app->>driver: aggregate with $search + non-stored fields
    driver->>mongod: aggregate command
    mongod->>server: gRPC command stream search
    activate server
    server->>command: mongot.search.command
    activate command
    command->>lucene: parse BSON, build query, collect text hits
    lucene-->>command: first batch of hit ids / scores
    command-->>server: encoded BSON response
    deactivate command
    server-->>mongod: search response batch of hit ids / scores
    deactivate server
    mongod->>idLookup: materialize matching collection documents by _id
    idLookup-->>mongod: matched MongoDB documents
    mongod->>server: killCursors when enough MongoT hits are buffered
    server-->>mongod: cursorsKilled
    mongod-->>driver: aggregate cursor batches
    driver-->>appProject: documents available from MongoDB cursor
    appProject-->>app: response with requested fields
```

### 5.5 Async Insert/Delete Indexing Trace

Insert/delete API calls do not become children of the client request trace inside MongoT. The client writes to MongoDB, then MongoT observes the write asynchronously through change streams and updates Lucene in background indexing traces. The clean mutation run used paired synthetic inserts/deletes and produced 5,662 inserts plus 5,662 deletes.

```mermaid
sequenceDiagram
    autonumber
    participant k6 as "k6: mutation workload"
    participant app as "java: client API"
    participant mongod as "mongod: MongoDB collection"
    participant change as "mongot: replication.steadystate"
    participant decode as "mongot: DecodingWorkScheduler"
    participant indexer as "mongot: IndexingWorkScheduler"
    participant writer as "mongot: SingleLuceneIndexWriter"
    participant lucene as "mongot: Lucene IndexWriter"
    participant refresh as "mongot: SearcherManager refresh"

    k6->>app: create synthetic document
    app->>mongod: insert document
    k6->>app: delete synthetic document
    app->>mongod: delete same document
    mongod-->>change: change stream batch
    activate change
    change->>decode: mongot.indexing.decode_batch
    activate decode
    decode->>change: decode change stream events
    change-->>indexer: document events
    deactivate decode
    deactivate change
    activate indexer
    indexer->>writer: document event INSERT
    writer->>writer: build document block
    writer->>lucene: IndexWriter.updateDocuments
    lucene-->>writer: inserted block indexed
    indexer->>writer: document event DELETE
    writer->>lucene: IndexWriter.deleteDocuments
    lucene-->>writer: matching documents deleted
    opt periodic commit
        writer->>lucene: IndexWriter.commit
        lucene-->>writer: durable segment/user data commit
    end
    indexer-->>refresh: maybeRefreshBlocking
    refresh-->>indexer: new searcher can see committed changes
    deactivate indexer
```

Measured mutation medians:

| Span | Median | Meaning |
| --- | ---: | --- |
| `mongot.indexing.change_stream_batch` | 299.5 us | Receives change stream batch for the index generation. |
| `mongot.indexing.batch` | 146 us | Runs scheduled indexing batch. |
| `mongot.indexing.document_event` INSERT | 168 us | Routes insert/update document event into Lucene writer. |
| `mongot.lucene.index_writer.update_documents` | 54 us | Actual Lucene `IndexWriter.updateDocuments` call. |
| `mongot.lucene.index_writer.delete_documents` | 3 us | Actual Lucene `IndexWriter.deleteDocuments` call. |
| `mongot.lucene.index_writer.commit` | 20.3 ms | Periodic Lucene commit when observed. |

### 5.6 Trace Hierarchy And Accounted Time

This diagram is entirely inside the `mongot` executable. The most important tracing lesson is that the root stream span and the initial command span answer different questions. The stream span represents end-to-end MongoT stream lifetime. The command span explains the initial command's internal work. Later cursor/getMore and client stream consumption can sit under the root stream but outside `mongot.search.command`.

```mermaid
sequenceDiagram
    autonumber
    participant root as "root stream span"
    participant command as "mongot.search.command"
    participant parse as "parse/build phase"
    participant lucene as "Lucene execution"
    participant bson as "BSON materialization"
    participant later as "later cursor/getMore"
    participant grpc as "gRPC stream lifecycle"

    root->>command: initial search command
    activate command
    command->>parse: prepare context + parse BSON + lookup index
    parse-->>command: query model and index metadata
    command->>lucene: build query + collect hits/candidates
    lucene-->>command: TopDocs / vector candidates
    command->>bson: materialize first BSON batch
    bson-->>command: first response batch
    command-->>root: command complete
    deactivate command
    par outside initial command span
        root->>later: cursor continuation / getMore work
        later->>lucene: searchAfter or advance batch producer
        lucene-->>later: later result batches
    and stream lifecycle
        root->>grpc: response observer, client consumption, half-close, cleanup
        grpc-->>root: stream complete
    end
```

### 5.7 Measured Scenario Comparison

This flowchart is a compact view of what changed across the load-test scenarios. It is not a call graph; it is a performance map from the 2026-04-26 trace breakdowns.

```mermaid
flowchart LR
    root["Client load tests<br/>25 VUs for search scenarios"]
    text["Text only<br/>166,253 requests<br/>554 req/s<br/>HTTP p50 39.2 ms<br/>MongoT stream p50 8.9 ms<br/>command p50 890 us"]
    vector["Vector only<br/>56,594 requests<br/>189 req/s<br/>HTTP p50 124 ms<br/>MongoT stream p50 10.0 ms<br/>command p50 2.8 ms<br/>vector collect p50 1.9 ms"]
    both["Text + vector<br/>23,388 requests<br/>77.8 req/s<br/>HTTP p50 268 ms<br/>MongoT stream p50 37.8 ms<br/>rankFusion splits into text and vector streams"]
    documentFetch["Document fetch<br/>67,688 requests<br/>225 req/s<br/>HTTP p50 87.0 ms<br/>MongoT stream p50 11.9 ms<br/>command p50 863 us"]
    mutations["Synthetic inserts/deletes<br/>11,324 mutation requests<br/>37.7 ops/s with 1 VU<br/>20,444 indexing trace hits<br/>async change stream to Lucene writer"]

    root --> text
    root --> vector
    root --> both
    root --> documentFetch
    root --> mutations

    text --> textLesson["Mostly sub-ms command work;<br/>BSON materialization visible"]
    vector --> vectorLesson["Lucene vector candidate collection<br/>dominates command phase"]
    both --> bothLesson["Two MongoT search streams:<br/>default + vector_caption"]
    documentFetch --> documentFetchLesson["End-to-end client/MongoDB time rises;<br/>MongoT command stays sub-ms"]
    mutations --> mutationLesson["Not a child of client trace;<br/>correlate by run window and indexing attributes"]
```
