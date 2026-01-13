# Steady State Replication

The `steadystate` package is responsible for the replication of indexes that are in the steady
state.

The entrypoint to the steady state subsystem is the `SteadyStateManager`, which manages the
lifecycle of replication for all indexes in the steady state. Indexes are added to the
`SteadyStateManager` via add(), and stopped via stop().

The `SteadyStateManager` registers the index with the `ChangeStreamManager`, which creates and
stores a `ChangeStreamIndexManager` to ensure that the index is updated with new documents.

The `ChangeStreamManager` will request batches in a round-robin fashion for each index registered
with it, up to the configured number of concurrent change streams. It will then use the
individual `ChangeStreamIndexManager` for the index to process the batch and schedule it to be
indexed on the `IndexingWorkScheduler`.

The `IndexingWorkScheduler` manages batches across different indexes for both steady state and
initial sync work. After a batch is processed, `IndexingWorkScheduler` updates сommit user data
in `DocumentIndexer`, while `PeriodicIndexComitter` schedules background commits with a configured
frequency. Each commit effectively includes all recently added documents and change stream resume
token from a previously processed batch. The `ChangeStreamManager` will not wait for a batch to
finish indexing before requesting a new batch for the same index. Instead,
the `IndexingWorkScheduler` will guarantee that batches for a given index are indexed in the order
they are submitted.