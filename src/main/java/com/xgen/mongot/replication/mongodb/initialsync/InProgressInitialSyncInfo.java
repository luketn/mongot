package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import java.util.concurrent.CompletableFuture;

/**
 * InitialSyncRequest is a data class representing the information about an initial sync that has
 * started.
 */
class InProgressInitialSyncInfo {

  /** The InitialSyncRequest that was serviced, starting the initial sync. */
  private final InitialSyncRequest request;

  /** The thread that the InitialSyncManager is running on. */
  private final Thread managerThread;

  InProgressInitialSyncInfo(InitialSyncRequest request, Thread managerThread) {
    this.request = request;
    this.managerThread = managerThread;
  }

  InitialSyncRequest getRequest() {
    return this.request;
  }

  CompletableFuture<ChangeStreamResumeInfo> getFuture() {
    return this.request.getOnCompleteFuture();
  }

  void cancel() {
    this.managerThread.interrupt();
  }
}
