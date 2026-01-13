package com.xgen.mongot.index.ingestion.stored;

import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * A {@link StoredBuilder} used to build the stored source {@code BsonDocument} to be stored in the
 * index.
 */
public class StoredDocumentBuilder implements StoredBuilder {
  final StoredSourceDefinition storedSourceDefinition;
  final BsonDocument document;

  private StoredDocumentBuilder(
      StoredSourceDefinition storedSourceDefinition, BsonDocument document) {
    this.storedSourceDefinition = storedSourceDefinition;
    this.document = document;
  }

  /**
   * Creates a {@link StoredBuilder}s to build stored source {@link BsonDocument}s, given the {@link
   * StoredSourceDefinition} of an index.
   */
  public static Optional<StoredBuilder> create(StoredSourceDefinition storedSourceDefinition) {
    if (storedSourceDefinition.isAllExcluded()) {
      return Optional.empty();
    }

    return Optional.of(new StoredDocumentBuilder(storedSourceDefinition, new BsonDocument()));
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    FieldPath absolutePath = FieldPath.newRoot(field);
    if (!this.storedSourceDefinition.isPathToStored(absolutePath)) {
      return Optional.empty();
    }

    return StoredFieldValueHandler.createForDocument(
        this.storedSourceDefinition, absolutePath, this.document);
  }

  @Override
  public Optional<BsonDocument> build() {
    return Optional.of(this.document);
  }
}
