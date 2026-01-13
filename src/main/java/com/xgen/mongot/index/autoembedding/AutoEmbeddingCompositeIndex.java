package com.xgen.mongot.index.autoembedding;

import static com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration.MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING;

import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.VectorIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import java.io.IOException;

/**
 * Composite Index type for AutoEmbeddingIndexGeneration used for consolidating index status from
 * both Materialized View and Lucene.
 */
public class AutoEmbeddingCompositeIndex implements VectorIndex {
  public final InitializedMaterializedViewIndex matViewIndex;
  public final VectorIndex vectorIndex;

  public AutoEmbeddingCompositeIndex(
      InitializedMaterializedViewIndex matViewIndex, VectorIndex vectorIndex) {
    this.matViewIndex = matViewIndex;
    this.vectorIndex = vectorIndex;
  }

  @Override
  public VectorIndexDefinition getDefinition() {
    return this.matViewIndex.getDefinition();
  }

  public VectorIndexDefinition getDerivedDefinition() {
    return this.vectorIndex.getDefinition();
  }

  @Override
  public void setStatus(IndexStatus status) {
    throw new UnsupportedOperationException(
        "Don't set status directly on AutoEmbedding composite index.");
  }

  @Override
  public IndexStatus getStatus() {
    IndexStatus matViewStatus = this.matViewIndex.getStatus();
    IndexStatus luceneStatus = this.vectorIndex.getStatus();
    // TODO(CLOUDP-356241): Implement this by matview lucene status mapping
    if (matViewStatus.canServiceQueries()) {
      return luceneStatus;
    }
    return matViewStatus;
  }

  @Override
  public boolean isCompatibleWith(IndexDefinition indexDefinition) {
    if (indexDefinition.isAutoEmbeddingIndex()
        && indexDefinition.asVectorDefinition().getParsedAutoEmbeddingFeatureVersion()
            >= MIN_VERSION_FOR_MATERIALIZED_VIEW_EMBEDDING) {
      return this.matViewIndex.isCompatibleWith(indexDefinition);
    }
    return this.vectorIndex.isCompatibleWith(indexDefinition);
  }

  @Override
  public void drop() throws IOException {
    this.matViewIndex.drop();
    this.vectorIndex.drop();
  }

  @Override
  public boolean isClosed() {
    return this.matViewIndex.isClosed() && this.vectorIndex.isClosed();
  }

  @Override
  public void throwIfUnavailableForQuerying() throws IndexUnavailableException {
    // TODO(CLOUDP-356241): Implement matview::throwIfUnavailableForQuerying
    this.vectorIndex.throwIfUnavailableForQuerying();
  }

  @Override
  public void close() throws IOException {
    this.matViewIndex.close();
    this.vectorIndex.close();
  }
}
