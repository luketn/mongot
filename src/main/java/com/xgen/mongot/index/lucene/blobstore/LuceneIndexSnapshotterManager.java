package com.xgen.mongot.index.lucene.blobstore;

import com.xgen.mongot.index.blobstore.BlobstoreSnapshotterManager;
import com.xgen.mongot.index.version.GenerationId;
import java.util.Optional;

/** BlobstoreSnapshotterManager extension that returns LuceneBlobstoreSnapshotter instances. */
public interface LuceneIndexSnapshotterManager extends BlobstoreSnapshotterManager {
  @Override
  Optional<? extends LuceneIndexSnapshotter> get(GenerationId generationId);
}
