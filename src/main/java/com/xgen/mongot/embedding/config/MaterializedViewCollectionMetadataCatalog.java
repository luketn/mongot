package com.xgen.mongot.embedding.config;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.MaterializedViewGenerationId;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache for MaterializedViewCollectionMetadata, keyed by GeneratorId, defined by
 * canonicalKey. This cache is populated at index creation time and used for both indexing and
 * query. The methods can take in either a GenerationId or a MaterializedViewGenerationId that
 * belongs to the same index generation.
 */
public class MaterializedViewCollectionMetadataCatalog {

  private final Map<String, MaterializedViewCollectionMetadata> metadataMap;

  public MaterializedViewCollectionMetadataCatalog() {
    this.metadataMap = new ConcurrentHashMap<>();
  }

  /**
   * Canonical key for mat view metadata, follows uniqueString() format of
   * MaterializedViewGenerationId by converting GenerationId to MaterializedViewGenerationId if
   * needed.
   */
  public static String canonicalKey(GenerationId generationId) {
    if (generationId instanceof MaterializedViewGenerationId matViewId) {
      return matViewId.uniqueString();
    }
    return MaterializedViewGenerationId.from(generationId).uniqueString();
  }

  /** Registers a Materialized View Metadata by MaterializedViewGenerationId. */
  public void addMetadata(
      MaterializedViewGenerationId generationId, MaterializedViewCollectionMetadata metadata) {
    this.metadataMap.put(canonicalKey(generationId), metadata);
  }

  /**
   * Returns MaterializedViewCollectionMetadata by GenerationId.
   *
   * <p>Note: No need to tighten the input GenerationId type
   */
  public MaterializedViewCollectionMetadata getMetadata(GenerationId generationId) {
    checkState(
        this.metadataMap.containsKey(canonicalKey(generationId)),
        "Mat view metadata not found for generationId: %s. This likely indicates a bug.",
        generationId);
    return this.metadataMap.get(canonicalKey(generationId));
  }

  /**
   * Checks whether there is MaterializedViewCollectionMetadata associated with the generationId.
   * This method takes a regular GenerationId so that we can use it to check all regular Indexes
   * during lifecycle events like dropIndex(GenerationId).
   */
  public Optional<MaterializedViewCollectionMetadata> getMetadataIfPresent(
      GenerationId generationId) {
    return Optional.ofNullable(this.metadataMap.get(canonicalKey(generationId)));
  }

  /**
   * Removes metadata only when a specific MaterializedViewGenerationId is no longer in replication,
   * should be called with the generationId from MaterializedViewGenerator after shutdown.
   */
  public void removeMetadata(MaterializedViewGenerationId generationId) {
    this.metadataMap.remove(canonicalKey(generationId));
  }
}
