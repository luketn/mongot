package com.xgen.mongot.embedding.config;

import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.UUID;

/**
 * Runtime data model for MaterializedView Collection Metadata, represents immutable properties of a
 * MaterializedView collection used for indexing and query.
 */
public record MaterializedViewCollectionMetadata(
    MaterializedViewSchemaMetadata schemaMetadata, UUID collectionUuid, String collectionName) {

  /** Materialized View Schema metadata for both indexing and query. */
  public record MaterializedViewSchemaMetadata(
      long materializedViewSchemaVersion, Map<FieldPath, FieldPath> autoEmbeddingFieldsMapping) {}
}
