package com.xgen.mongot.index.ingestion.stored;

import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Contains logic to create {@link FieldValueHandler} for stored fields, given a leaf path. Similar
 * to {@link StoredDocumentBuilder}, except is not used at the root of a source BSON document.
 */
class StoredDocumentHandler implements DocumentHandler {
  final StoredSourceDefinition storedSourceDefinition;
  final FieldPath absolutePath;
  final BsonDocument document;

  StoredDocumentHandler(
      StoredSourceDefinition storedSourceDefinition,
      FieldPath absolutePath,
      BsonDocument document) {
    this.storedSourceDefinition = storedSourceDefinition;
    this.absolutePath = absolutePath;
    this.document = document;
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    return StoredFieldValueHandler.createForDocument(
        this.storedSourceDefinition, this.absolutePath.newChild(field), this.document);
  }
}
