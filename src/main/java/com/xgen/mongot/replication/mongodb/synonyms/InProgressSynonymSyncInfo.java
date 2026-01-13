package com.xgen.mongot.replication.mongodb.synonyms;

import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;

public class InProgressSynonymSyncInfo {
  final SynonymSyncRequest request;
  final Thread syncThread;

  public InProgressSynonymSyncInfo(SynonymSyncRequest request, Thread syncThread) {
    this.request = request;
    this.syncThread = syncThread;
  }

  SynonymSyncRequest getRequest() {
    return this.request;
  }

  void cancel() {
    this.request.getFuture().completeExceptionally(SynonymSyncException.createShutDown());
    this.syncThread.interrupt();
  }
}
