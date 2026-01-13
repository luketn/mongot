package com.xgen.mongot.index.ingestion.stored;

import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * A {@link DocumentHandler} that knows how to build the stored source document to be stored in an
 * index.
 */
public interface StoredBuilder extends DocumentHandler {
  Optional<BsonDocument> build();

  default DocumentHandler asDocumentHandler() {
    return this;
  }
}
