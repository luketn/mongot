package com.xgen.mongot.index.lucene.blobstore;

import com.xgen.mongot.index.blobstore.IndexBlobstoreSnapshotter;
import org.apache.lucene.index.SnapshotDeletionPolicy;

/** Extends IndexBlobstoreSnapshotter to provide snapshot deletion policies for Lucene indexes. */
public interface LuceneIndexSnapshotter extends IndexBlobstoreSnapshotter {
  /**
   * Returns the snapshot deletion policy for the given partition index.
   *
   * @param partitionIndex the index of the partition
   * @return the snapshot deletion policy for that partition
   */
  SnapshotDeletionPolicy getSnapshotDeletionPolicy(int partitionIndex);
}
