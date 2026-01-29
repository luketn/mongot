package com.xgen.mongot.embedding.utils;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Clones a BsonDocument, replacing vector text fields with embeddings.
 *
 * <p>When run with the BsonDocumentProcessor, it will clone a RawBsonDocument into the
 * BsonDocument given in create. It will also replace any vector text fields in the given
 * VectorIndexFieldMapping with the BsonValue in the given embeddings.
 */
public class ReplaceStringsDocumentHandler implements DocumentHandler {

  private final VectorIndexFieldMapping mapping;
  private final Optional<FieldPath> path;
  private final BsonValue bsonValue;
  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField;
  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> existingEmbeddings;

  private ReplaceStringsDocumentHandler(
      VectorIndexFieldMapping mapping,
      Optional<FieldPath> path,
      BsonValue bsonValue,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> existingEmbeddings) {
    this.mapping = mapping;
    this.path = path;
    this.bsonValue = bsonValue;
    this.embeddingsPerField = embeddingsPerField;
    this.existingEmbeddings = existingEmbeddings;
  }

  public static ReplaceStringsDocumentHandler create(
      VectorIndexFieldMapping mapping,
      Optional<FieldPath> path,
      BsonDocument bsonDoc,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> existingEmbeddings) {
    return new ReplaceStringsDocumentHandler(
        mapping, path, bsonDoc, embeddingsPerField, existingEmbeddings);
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    FieldPath fullPath = createChildFieldPath(field);
    if (!this.mapping.childPathExists(fullPath)) {
      return Optional.empty();
    }
    return Optional.of(
        ReplaceStringsFieldValueHandler.create(
            this.mapping, fullPath, this.bsonValue, this.embeddingsPerField,
            this.existingEmbeddings));
  }

  private FieldPath createChildFieldPath(String field) {
    return this.path.map(path -> path.newChild(field)).orElseGet(() -> FieldPath.newRoot(field));
  }
}
