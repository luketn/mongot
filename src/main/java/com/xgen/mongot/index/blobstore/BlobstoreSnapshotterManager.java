package com.xgen.mongot.index.blobstore;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.version.GenerationId;
import java.util.Optional;

public interface BlobstoreSnapshotterManager {
  /** Returns the snapshotter for the index, if present. */
  Optional<? extends IndexBlobstoreSnapshotter> get(GenerationId generationId);

  /** Creates a snapshotter for the given index. Must be invoked only once per index. */
  void add(IndexGeneration indexGeneration);

  /** Remove the index snapshotter. */
  void drop(GenerationId generationId);

  /** Schedule periodic upload to blobstore via the snapshotter for the index. */
  void scheduleUpload(IndexGeneration indexGeneration);

  /** Returns true if downloads should be attempted */
  boolean areDownloadsEnabled();

  /** Performs shutdown of the manager and cancels all scheduled uploads. */
  void shutdown();
}
