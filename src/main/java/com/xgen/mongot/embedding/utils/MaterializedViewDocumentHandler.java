package com.xgen.mongot.embedding.utils;


import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

// TODO(CLOUDP-363302): Support stored source from vectorIndexDefinition.
/**
 * Clones a BsonDocument but only keeps filter fields and stored source.
 *
 * <p>When run with the BsonDocumentProcessor, it will clone a RawBsonDocument into the BsonDocument
 * given in constructor.
 */
public class MaterializedViewDocumentHandler implements DocumentHandler {

  private final VectorIndexFieldMapping filteredMapping;
  private final Optional<FieldPath> path;
  private final BsonValue bsonValue;

  private MaterializedViewDocumentHandler(
      VectorIndexFieldMapping filteredMapping, Optional<FieldPath> path, BsonValue bsonValue) {
    this.filteredMapping = filteredMapping;
    this.path = path;
    this.bsonValue = bsonValue;
  }

  public static MaterializedViewDocumentHandler create(
      VectorIndexFieldMapping filteredMapping, Optional<FieldPath> path, BsonDocument bsonDoc) {
    return new MaterializedViewDocumentHandler(filteredMapping, path, bsonDoc);
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    FieldPath fullPath = createChildFieldPath(field);
    if (!this.filteredMapping.childPathExists(fullPath)) {
      return Optional.empty();
    }
    return Optional.of(
        MaterializedViewFieldValueHandler.create(this.filteredMapping, fullPath, this.bsonValue));
  }

  private FieldPath createChildFieldPath(String field) {
    return this.path.map(path -> path.newChild(field)).orElseGet(() -> FieldPath.newRoot(field));
  }
}
