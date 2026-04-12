package com.xgen.mongot.embedding;

import com.xgen.mongot.util.Check;

/**
 * Contextual information for an embedding request.
 *
 * <p>This includes metadata about the source of the embedding request, such as the database, index
 * name and collection name, which can be used for multi-tenant credential lookup, billing or 
 * general logging.
 */
public record EmbeddingRequestContext(String database, String indexName, String collectionName) {

  public EmbeddingRequestContext {
    Check.argNotNull(database, "database");
    Check.argNotNull(indexName, "indexName");
    Check.argNotNull(collectionName, "collectionName");
  }
}
