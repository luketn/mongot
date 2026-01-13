package com.xgen.mongot.embedding;

import com.xgen.mongot.util.Check;
import java.util.UUID;
import org.bson.types.ObjectId;

/**
 * Contextual information for an embedding request.
 *
 * <p>This includes metadata about the source of the embedding request, such as the database, index,
 * and collection, which can be used for multi-tenant credential lookup, billing or general logging.
 */
public record EmbeddingRequestContext(String database, ObjectId indexId, UUID collectionUuid) {

  public EmbeddingRequestContext {
    Check.argNotNull(database, "database");
    Check.argNotNull(indexId, "indexId");
    Check.argNotNull(collectionUuid, "collectionUuid");
  }
}
