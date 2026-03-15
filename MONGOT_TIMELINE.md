# MongoT Runtime Timeline

This timeline is derived from the package tour plus the actual bootstrap, config, lifecycle, replication, cursor, and server code paths.

Arrows are package-level flows, backed by representative classes such as:

- `community`: `MongotCommunity`, `CommunityMongotBootstrapper`
- `config`: `DefaultConfigManager`, `CommunityConfigUpdater`, `PeriodicConfigMonitor`
- `lifecycle`: `DefaultLifecycleManager`, `IndexLifecycleManager`
- `replication`: `MongoDbReplicationManager`, `ReplicationIndexManager`, `InitialSyncQueue`, `ChangeStreamManager`
- `server`: `GrpcStreamingServer`, `ServerCallHandler`, `SearchCommand`, `VectorSearchCommand`
- `cursor`: `MongotCursorManagerImpl`, `IndexCursorManagerImpl`, `CursorFactory`
- `index`: initialized index creation plus Lucene-backed readers/writers
- `catalogservice`: authoritative catalog and metadata service

The startup, indexing, steady-state, and shutdown flows below are grounded in code. The later Atlas Search and Vector Search load stages are modeled from the serving paths so their different dependencies stay visible in the graph, not from a captured production trace.

## 1. Bootstrap, Control Plane, and Service Initialization

MongoT starts by letting `community` hand control to `config`, which fans out into logging, metrics, monitors, metadata, cursors, and serving infrastructure.

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

Once the process is up, lifecycle and replication cooperate to initialize indexes, establish durable resume state, and move each generation into steady service.

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

Stable serving splits into two distinct query paths: Atlas Search uses the server-cursor-index chain, while Vector Search adds the embedding service before the index reader.

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

Shutdown unwinds the same stack in reverse order so serving, telemetry, config, lifecycle, and replication all stop cleanly without orphaned work.

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

## Animation Scenario

The tour app animation should treat this as a compressed runtime story with multiple concurrent streams:

| Virtual Time | Phase | Main Streams |
| --- | --- | --- |
| `0s-6s` | Bootstrap | `community -> config -> metrics/monitor/catalogservice/cursor/server` |
| `6s-18s` | Initialize | `config -> lifecycle -> index -> replication` |
| `18s-34s` | Initial sync | `replication.initialsync -> index` plus buffered write capture |
| `34s-44s` | Stable state | `replication.steadystate -> index` with config and metadata polling in the background |
| `44s-59s` | Atlas Search load | `server -> cursor -> index` Atlas Search flow with steady-state replication continuing behind it |
| `59s-74s` | Vector Search load | `server -> embedding -> index` Vector Search flow with steady-state replication continuing behind it |
| `74s-78s` | Atlas Search taper | The Atlas Search path remains active, but at visibly reduced intensity |
| `78s-82s` | Vector Search taper | The Vector Search path remains active, but at visibly reduced intensity |
| `82s-90s` | Shutdown | `community -> server/metrics/config -> lifecycle -> replication -> catalogservice/logging` |

### Edge Groups to Animate

- Bootstrap and initialize:
  - `community -> config`
  - `config -> metrics`
  - `config -> monitor`
  - `config -> catalogservice`
  - `config -> cursor`
  - `config -> server`
  - `config -> lifecycle`
- Index creation and initial replication:
  - `lifecycle -> index`
  - `lifecycle -> replication`
  - `replication -> index`
- Stable state:
  - `config -> catalogservice`
  - `catalogservice -> config`
  - `replication -> index`
  - `monitor -> config`
- Atlas Search load:
  - `server -> cursor`
  - `cursor -> index`
- Vector Search load:
  - `server -> embedding`
  - `embedding -> index`
- Shutdown:
  - `community -> server`
  - `community -> metrics`
  - `community -> config`
  - `config -> lifecycle`
  - `lifecycle -> replication`
  - `community -> catalogservice`
  - `community -> logging`

### Notes

- The package graph is import-oriented, so a few animation edges represent runtime call direction rather than the static import arrow direction.
- The steady-state and config-monitor streams are genuinely concurrent in code.
- The `44s-74s` query-load window is intentionally split into separate Atlas Search and Vector Search slices so the visualization can show the Lucene/cursor path and the embedding/vector path independently.
