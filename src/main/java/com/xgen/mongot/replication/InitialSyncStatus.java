package com.xgen.mongot.replication;

import java.time.Instant;

/**
 * The status of initial sync builds.
 *
 * @param startTime when the initial sync status was created.
 * @param isInitialSyncPaused whether the initial sync is paused due to high disk utilization.
 */
public record InitialSyncStatus(Instant startTime, boolean isInitialSyncPaused) {}
