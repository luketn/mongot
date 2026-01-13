# Change Stream
The `changestream` package is responsible for managing the tailing of change streams for indexes in steady state replication.

The entrypoint to the change stream subsystem is through the `ChangeStreamManager`, which manages 
the lifecycle of the change stream tailing for all indexes in steady state. Indexes are
added to the `ChangeStreamManager` via `add()`, and stopped via `stop()`.

The change stream manager has a `ChangeStreamDispatcher` that is in charge of ensuring that only
the configured number of change streams are being queried on the mongod at any given time. This
is accomplished by running a dispatching thread with a semaphore and a queue, similar to the
`InitialSyncDispatcher`.

The actual change stream tailing is accomplished using the MongoDB sync driver, and calls
the aggregate and getMore commands explicitly via `db.runCommand()` instead of using the driver's
change stream API. This allows us finer grained control over exactly when we request the mongod
to do work on our behalf, as well as allows us to process a whole batch of change stream events
at a time, rather then one at a time.

When an index is added to the `ChangeStreamManager`, a `ChangeStreamIndexManager` and
``ChangeStreamMongoClient`` are created for it, and is enqueued onto the `ChangeStreamDispatcher`'s
request queue. When there is sufficient capacity for the request, the `ChangeStreamDispatcher` will
dequeue the request, use the `ChangeStreamMongoClient` to fulfill it, and enqueue the batch to be
indexed by scheduling its `ChangeStreamBatchIndexer::indexBatch`' method to be run on its Executor via
the `ChangeStreamIndexManager`. When the indexing completes, the `ChangeStreamDispatcher` enqueues it
back onto its requests queue.

The performance characteristics of the change stream subsystem rely on tuning two parameters:
`numConcurrentChangeStreams` and `changeStreamMaxTimeMs`. `numConcurrentChangeStreams` is the amount of
`getMore`s that are allowed to be running on the mongod at any given time, and
`changeStreamMaxTimeMs` is the amount of time each individual getMore is allowed to run on the
mongod before returning.

These settings, in conjunction with the number of configured indexes, impact the expected
interval between servicing any given index. For example, if there are `65` indexes,
`numConcurrentChangeStreams` is `4`, and `changeStreamMaxTimeMs` is `1000`, then after a given index
successfully runs a getMore, it is expected that it will take `(64 / 4) * 1000 ms ( = 16s)` before
the index runs another getMore.

In the future, it may instead set a target interval and concurrency level, and dynamically
change the `changeStreamMaxTimeMs` based on the number of indexes.
