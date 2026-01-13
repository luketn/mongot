package com.xgen.mongot.index.ingestion.handlers;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * Groups two {@link DocumentHandler} or {@link FieldValueHandler}s together, handling values for
 * both and returning appropriate results when asked if a field, sub-array, or sub-document should
 * be indexed by delegating to the two wrapped handlers.
 */
public class Composite {
  public static class CompositeDocumentHandler implements DocumentHandler {
    final DocumentHandler first;
    final DocumentHandler second;

    private CompositeDocumentHandler(DocumentHandler first, DocumentHandler second) {
      this.first = first;
      this.second = second;
    }

    public static Optional<DocumentHandler> of(
        Optional<DocumentHandler> first, Optional<DocumentHandler> second) {
      if (first.isEmpty()) {
        return second;
      }
      if (second.isEmpty()) {
        return first;
      }

      return Optional.of(new CompositeDocumentHandler(first.get(), second.get()));
    }

    @Override
    public Optional<FieldValueHandler> valueHandler(String field) {
      return CompositeFieldValueHandler.of(
          this.first.valueHandler(field), this.second.valueHandler(field));
    }
  }

  public static class CompositeFieldValueHandler implements FieldValueHandler {
    final FieldValueHandler first;
    final FieldValueHandler second;

    private CompositeFieldValueHandler(FieldValueHandler first, FieldValueHandler second) {
      this.first = first;
      this.second = second;
    }

    public static Optional<FieldValueHandler> of(
        Optional<FieldValueHandler> first, Optional<FieldValueHandler> second) {
      if (first.isEmpty()) {
        return second;
      }
      if (second.isEmpty()) {
        return first;
      }
      return Optional.of(new CompositeFieldValueHandler(first.get(), second.get()));
    }

    @Override
    public void handleBinary(Supplier<Binary> supplier) {
      this.first.handleBinary(supplier);
      this.second.handleBinary(supplier);
    }

    @Override
    public void handleBoolean(Supplier<Boolean> supplier) {
      this.first.handleBoolean(supplier);
      this.second.handleBoolean(supplier);
    }

    @Override
    public void handleDateTime(Supplier<Long> supplier) {
      this.first.handleDateTime(supplier);
      this.second.handleDateTime(supplier);
    }

    @Override
    public void handleDouble(Supplier<Double> supplier) {
      this.first.handleDouble(supplier);
      this.second.handleDouble(supplier);
    }

    @Override
    public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
      this.first.handleGeometry(supplier);
      this.second.handleGeometry(supplier);
    }

    @Override
    public void handleInt32(Supplier<Integer> supplier) {
      this.first.handleInt32(supplier);
      this.second.handleInt32(supplier);
    }

    @Override
    public void handleInt64(Supplier<Long> supplier) {
      this.first.handleInt64(supplier);
      this.second.handleInt64(supplier);
    }

    @Override
    public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
      this.first.handleKnnVector(supplier);
      this.second.handleKnnVector(supplier);
    }

    @Override
    public void handleNull() {
      this.first.handleNull();
      this.second.handleNull();
    }

    @Override
    public void handleObjectId(Supplier<ObjectId> supplier) {
      this.first.handleObjectId(supplier);
      this.second.handleObjectId(supplier);
    }

    @Override
    public void handleString(Supplier<String> supplier) throws IOException {
      this.first.handleString(supplier);
      this.second.handleString(supplier);
    }

    @Override
    public void handleUuid(Supplier<Optional<UUID>> supplier) {
      this.first.handleUuid(supplier);
      this.second.handleUuid(supplier);
    }

    @Override
    public void handleRawBsonValue(Supplier<BsonValue> supplier) {
      this.first.handleRawBsonValue(supplier);
      this.second.handleRawBsonValue(supplier);
    }

    @Override
    public void markFieldNameExists() {
      this.first.markFieldNameExists();
      this.second.markFieldNameExists();
    }

    @Override
    public Optional<FieldValueHandler> arrayFieldValueHandler() {
      return CompositeFieldValueHandler.of(
          this.first.arrayFieldValueHandler(), this.second.arrayFieldValueHandler());
    }

    @Override
    public Optional<DocumentHandler> subDocumentHandler() {
      return CompositeDocumentHandler.of(
          this.first.subDocumentHandler(), this.second.subDocumentHandler());
    }
  }
}
