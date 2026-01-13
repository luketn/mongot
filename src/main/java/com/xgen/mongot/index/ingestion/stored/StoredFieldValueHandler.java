package com.xgen.mongot.index.ingestion.stored;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
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

/** Contains logic to insert values into stored source. */
class StoredFieldValueHandler implements FieldValueHandler {
  final StoredSourceDefinition storedSourceDefinition;
  final FieldPath absolutePath;
  final boolean isStored;
  final Consumer<BsonValue> valueSink;

  private StoredFieldValueHandler(
      StoredSourceDefinition storedSourceDefinition,
      FieldPath absolutePath,
      boolean isStored,
      Consumer<BsonValue> valueSink) {
    this.storedSourceDefinition = storedSourceDefinition;
    this.absolutePath = absolutePath;
    this.isStored = isStored;
    this.valueSink = valueSink;
  }

  static Optional<FieldValueHandler> createForDocument(
      StoredSourceDefinition storedSourceDefinition, FieldPath absolutePath, BsonDocument parent) {
    return create(
        storedSourceDefinition,
        absolutePath,
        bsonValue -> parent.append(absolutePath.getLeaf(), bsonValue));
  }

  static Optional<FieldValueHandler> createForArray(
      StoredSourceDefinition storedSourceDefinition, FieldPath absolutePath, BsonArray parent) {
    return create(storedSourceDefinition, absolutePath, parent::add);
  }

  private static Optional<FieldValueHandler> create(
      StoredSourceDefinition storedSourceDefinition,
      FieldPath absolutePath,
      Consumer<BsonValue> valueSink) {
    if (!storedSourceDefinition.isPathToStored(absolutePath)) {
      // If the absolute path is not a path to a stored field, return an empty optional and do not
      // populate stored fields under that path.
      return Optional.empty();
    }

    return Optional.of(
        new StoredFieldValueHandler(
            storedSourceDefinition,
            absolutePath,
            storedSourceDefinition.isStored(absolutePath),
            valueSink));
  }

  <T> void handleValueIfStored(
      Supplier<T> supplier, Function<T, ? extends BsonValue> bsonValueConverter) {
    if (this.isStored) {
      this.valueSink.accept(bsonValueConverter.apply(supplier.get()));
    }
  }

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    // Use a new StoredValueHandler with an array container to store multi-values at this path.
    BsonArray newParent = new BsonArray();
    this.valueSink.accept(newParent);
    return StoredFieldValueHandler.createForArray(
        this.storedSourceDefinition, this.absolutePath, newParent);
  }

  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    // Use a new StoredFieldHandler to create value handlers and index field values of this sub
    // document.
    BsonDocument newParent = new BsonDocument();
    this.valueSink.accept(newParent);
    return Optional.of(
        new StoredDocumentHandler(this.storedSourceDefinition, this.absolutePath, newParent));
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    handleValueIfStored(supplier, binary -> new BsonBinary(binary.getType(), binary.getData()));
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    handleValueIfStored(supplier, BsonBoolean::new);
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    handleValueIfStored(supplier, BsonDateTime::new);
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    handleValueIfStored(supplier, BsonDouble::new);
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
    // BSON has no native representation for geometries, so do nothing.
  }

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    handleValueIfStored(supplier, BsonInt32::new);
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    handleValueIfStored(supplier, BsonInt64::new);
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
    // do nothing (handled on array element level)
  }

  @Override
  public void handleNull() {
    handleValueIfStored(() -> BsonNull.VALUE, Function.identity());
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    handleValueIfStored(supplier, BsonObjectId::new);
  }

  @Override
  public void handleString(Supplier<String> supplier) {
    handleValueIfStored(supplier, BsonString::new);
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    // do nothing, since we already store the value via handleBinary() above
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
    handleValueIfStored(supplier, Function.identity());
  }

  @Override
  public void markFieldNameExists() {
    // do nothing
  }
}
