package com.xgen.mongot.embedding.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.Vector;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * Replaces strings values with vectors for Vector Text indexes.
 *
 * <p>If the given path is not in the given VectorIndexFieldMapping, then the original value is
 * added to the given BsonDocument or BsonArray. For Vector text fields defined in the
 * VectorIndexFieldMapping, the corresponding BsonValue in the given embeddings maps is returned. If
 * the text value is not a String or the embedding entry is not found, then nothing is added. *
 */
public class ReplaceStringsFieldValueHandler implements FieldValueHandler {

  public static final String HASH_FIELD_SUFFIX = "_hash";

  private final VectorIndexFieldMapping mapping;
  private final FieldPath path;
  private final BsonValue bsonValue;
  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField;
  private final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> existingEmbeddings;

  private ReplaceStringsFieldValueHandler(
      VectorIndexFieldMapping mapping,
      FieldPath path,
      BsonValue bsonValue,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> existingEmbeddings) {
    this.mapping = mapping;
    this.path = path;
    this.bsonValue = bsonValue;
    this.embeddingsPerField = embeddingsPerField;
    this.existingEmbeddings = existingEmbeddings;
  }

  public static FieldValueHandler create(
      VectorIndexFieldMapping mapping,
      FieldPath path,
      BsonValue bsonValue,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> embeddingsPerField,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> existingEmbeddings) {
    Check.checkArg(
        (bsonValue instanceof BsonDocument) || (bsonValue instanceof BsonArray),
        "bsonValue input must be either BsonDocument or BsonArray");
    return new ReplaceStringsFieldValueHandler(
        mapping, path, bsonValue, embeddingsPerField, existingEmbeddings);
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    if (!isVectorTextField()) {
      Binary binary = supplier.get();
      add(new BsonBinary(binary.getType(), binary.getData()));
    }
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    if (!isVectorTextField()) {
      add(new BsonBoolean(supplier.get()));
    }
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    if (!isVectorTextField()) {
      add(new BsonDateTime(supplier.get()));
    }
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    if (!isVectorTextField()) {
      add(new BsonDouble(supplier.get()));
    }
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
  }

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    if (!isVectorTextField()) {
      add(new BsonInt32(supplier.get()));
    }
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    if (!isVectorTextField()) {
      add(new BsonInt64(supplier.get()));
    }
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
    if (!isVectorTextField()) {
      Optional<Vector> vector = supplier.get();
      if (vector.isPresent()) {
        add(BsonVectorParser.encode(vector.get()));
      }
    }
  }

  @Override
  public void handleNull() {
    if (!isVectorTextField()) {
      add(new BsonNull());
    }
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    if (!isVectorTextField()) {
      add(new BsonObjectId(supplier.get()));
    }
  }

  @Override
  public void handleString(Supplier<String> supplier) {
    String textValue = supplier.get();
    if (isVectorTextField()) {
      if (this.existingEmbeddings.containsKey(this.path)
          && this.existingEmbeddings.get(this.path).containsKey(textValue)) {
        add(BsonVectorParser.encode(this.existingEmbeddings.get(this.path).get(textValue)));
        addContentHash(textValue);
      } else if (this.embeddingsPerField.containsKey(this.path)
          && this.embeddingsPerField.get(this.path).containsKey(textValue)) {
        add(BsonVectorParser.encode(this.embeddingsPerField.get(this.path).get(textValue)));
        addContentHash(textValue);
      } // else ignore if embedding not found
    } else {
      add(new BsonString(textValue));
    }
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    if (!isVectorTextField()) {
      Optional<UUID> uuid = supplier.get();
      if (uuid.isPresent()) {
        add(new BsonBinary(uuid.get()));
      }
    }
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
    if (!isVectorTextField()) {
      add(supplier.get());
    }
  }

  @Override
  public void markFieldNameExists() {
  }

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    BsonArray childBsonArray = new BsonArray();
    add(childBsonArray);
    return Optional.of(
        create(this.mapping, this.path, childBsonArray, this.embeddingsPerField,
            this.existingEmbeddings));
  }

  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    if (!isVectorTextField()) {
      BsonDocument childBsonDocument = new BsonDocument();
      add(childBsonDocument);
      return Optional.of(
          ReplaceStringsDocumentHandler.create(
              this.mapping,
              Optional.of(this.path),
              childBsonDocument,
              this.embeddingsPerField,
              this.existingEmbeddings));
    }
    return Optional.empty();
  }

  private void add(BsonValue childValue) {
    switch (this.bsonValue) {
      case BsonDocument bsonDocument -> bsonDocument.append(this.path.getLeaf(), childValue);
      case BsonArray bsonArray -> bsonArray.add(childValue);
      default -> throw new IllegalStateException(
          "Unexpected bsonValue type: " + this.bsonValue.getBsonType());
    }
  }

  private void addContentHash(String childValue) {
    if (this.bsonValue.isDocument()) {
      this.bsonValue
          .asDocument()
          .append(
              this.path.getLeaf() + HASH_FIELD_SUFFIX, new BsonString(computeTextHash(childValue)));
    } else {
      throw new IllegalStateException("Unexpected bsonValue type: " + this.bsonValue.getBsonType());
    }
  }

  private boolean isVectorTextField() {
    Optional<VectorIndexFieldDefinition> fieldDefinition =
        this.mapping.getFieldDefinition(this.path);
    return fieldDefinition
        .filter(
            vectorFieldDefinition ->
                vectorFieldDefinition.getType() == VectorIndexFieldDefinition.Type.TEXT
                    || vectorFieldDefinition.getType()
                    == VectorIndexFieldDefinition.Type.AUTO_EMBED)
        .isPresent();
  }

  /**
   * Computes a SHA-256 hash of the given text value and returns it as a hex string.
   *
   * @param text The text to hash
   * @return The SHA-256 hash as a hex string
   */
  public static String computeTextHash(String text) {
    return Hashing.sha256().hashString(text, StandardCharsets.UTF_8).toString();
  }
}
