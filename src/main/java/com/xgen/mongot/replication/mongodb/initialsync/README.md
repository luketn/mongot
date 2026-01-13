# Initial Sync

The `initialsync` package implements an initial sync of an Index from a MongoDB collection.

The entrypoint to running an initial sync is the `InitialSyncQueue`. Library consumers can enqueue
initial syncs to be asynchronously scheduled and run, or can cancel an initial sync that was
previously scheduled.

When an initial sync is enqueued via the `InitialSyncQueue`, it places the request on an unbounded
`BlockingQueue` that is shared with the `InitialSyncDispatcher`, which is running in a separate
thread. The `InitialSyncDispatcher` ensures that only up to the configured number of concurrent
initial syncs are running at any given time. When there is capacity to run an initial sync, the
`InitialSyncDispatcher` will pull a waiting request from the head of the queue, and schedule it to
on its own thread via an `InitialSyncManager`.

The `InitialSyncManager` is in charge of actually running the initial sync. It will create and
initialize a `ChangeStreamBuffer`, then spawn a `ChangeStreamBufferManager` on a separate thread to
buffer change stream events. At the same time, the `InitialSyncManager` will use a
`CollectionScanner` to do a collection scan in its own thread. Finally, after
the `CollectionScanner`
is finished and the `ChangeStreamBufferManager` has been shut down, the manager will run a
`ChangeStreamBufferApplier` in its own thread.

The `ChangeStreamBufferManager` will buffer events as long as the `CollectionScanner` is running.
When the scan is finished, it will stop buffering when it has reached and buffered events that
occurred after the reported optime of the collection scan. The `ChangeStreamBufferManager` buffers
these changes in a local collection on the co-located mongod instance, which will be read by the
`ChangeStreamBufferApplier` later.

During the initial sync, the index is periodically committed by per-index `PeriodicIndexCommitter`.
The data stored in the index user commit data during these commits contain the previously mentioned
information necessary to resume the initial sync in case of an expected or unexpected shutdown of
mongot. The index is once again committed after the initial sync completes. All commits happen
normally regardless of whether this initial sync was a clean start or a resume.

If an incomplete initial sync is being resumed, a resume data structure is passed to
the `InitialSyncQueue` when the initial sync is enqueued, and passed to the `InitialSyncManager`
when the initial sync begins running. This data structure contains, at a minimum, the operation time
at which the change stream of the initial sync being resumed began. If the interrupted initial sync
was performing the collection scan, a recently indexed `_id` is stored as a resume point, and is
passed to the `CollectionScanner` to use as a lower bound in the scan query. If the initial sync to
be resumed was interrupted while applying change stream events, we store both the operation time at
which the collection scan originally finished, and resume token of the last indexed change stream
batch. In this case, we skip instantiating the `CollectionScanner`, begin buffering change stream
events using the resume token from the initial sync resume info, immediately signal the
`ChangeStreamBufferManager` to stop buffering events after the point where the original collection
scan finished, and begin applying the buffered events.

When the `InitialSyncManager` completes, the future returned from `InitialSyncQueue::enqueue`
will be completed, successfully or exceptionally.

Both the `CollectionScanner` and the `ChangeStreamBufferApplier` use the `IndexingWorkScheduler`
to schedule indexing of batches of documents. The scheduler manages batches across different indexes
for both steady state and initial sync work. For each batch, initial sync classes create a resume
data payload, which is updated in `DocumentIndexer` via the `IndexingWorkScheduler` and later used
for periodic background commits. Additionally, once both the `CollectionScanner`
and `ChangeStreamBufferApplier` finish their work, we use the `DocumentIndexer` to commit the full
initial sync, with a change stream token which is used to begin steady state.

## Initial Sync and Optimes

An important detail about MongoDB operation times (also referred to as "cluster time", though the
latter more accurately refers to timestamps used to logically order operations in the context of a
replica set), is that the current optime at the server changes only when a write operation occurs.
In other words, we can consider a optime returned by Mongo to be a timestamp with an associated
write.

The initial sync process keeps track of optimes to provide resumeability and ensure consistency by
coordinating the collection scan and change stream buffering & application. At the beginning of the
initial sync process, we query the server for the current [readConcernMajorityOpTime][1], and save
this timestamp as the start of the initial sync (let's call this timestamp `N`). The collection scan
uses time `N` as the read concern `afterClusterTime` parameter, to ensure that our scan reflects the
changes made by the write at time `N`. The change stream, however, is started at time `N+1` (one
tick beyond time `N`). This distinction is made because the `startAtOperationTime` used when opening
a change stream is inclusive of the write at the given optime, and that write is already included in
the view of the collection being scanned. While this duplicate event does not affect correctness, it
is necessary to ensure that collections where the last write was a change stream invalidate event
can still be synced.

Another important, albeit slightly tangential, distinction to make, is that the `operationTime`
returned by most commands is the cluster time at which the (read, write, aggregate) operation ran,
which, for change streams, is unrelated to the time at which the change stream events occurred, and
unrelated to the point in time at which the change stream began.

[1]: https://docs.mongodb.com/manual/reference/command/replSetGetStatus/#mongodb-data-replSetGetStatus.optimes.readConcernMajorityOpTime