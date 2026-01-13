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
 * Implements the brunt of indexing logic, and is responsible for:
 *
 * <ul>
 *   <li>indexing a value
 *   <li>creating {@link FieldValueHandler}s for sub-arrays
 *   <li>creating {@link DocumentHandler}s for sub-documents
 * </ul>
 *
 * <p>When creating "child" handlers, {@link FieldValueHandler} behaves in a similar way to {@link
 * DocumentHandler} - child handlers are instantiated with immutable state, and are created with
 * configuration relevant to the path of the source document they are responsible for indexing.
 */
public interface FieldValueHandler {
  void handleBinary(Supplier<Binary> supplier);

  void handleBoolean(Supplier<Boolean> supplier);

  void handleDateTime(Supplier<Long> supplier);

  void handleDouble(Supplier<Double> supplier);

  void handleGeometry(Supplier<Optional<Geometry>> supplier);

  void handleInt32(Supplier<Integer> supplier);

  void handleInt64(Supplier<Long> supplier);

  void handleKnnVector(Supplier<Optional<Vector>> supplier);

  void handleNull();

  void handleObjectId(Supplier<ObjectId> supplier);

  void handleString(Supplier<String> supplier) throws IOException;

  void handleUuid(Supplier<Optional<UUID>> supplier);

  /**
   * Called to handle a value type not handled with any other, more specific "handleX" method of
   * this {@link FieldValueHandler}.
   */
  void handleRawBsonValue(Supplier<BsonValue> supplier);

  /**
   * It is expected that this method should be called on every field name that exists, regardless of
   * whether that field has a value that is handled by some other method in this class.
   */
  void markFieldNameExists();

  /**
   * Get a {@link FieldValueHandler} that is configured to handle multi-valued elements at the
   * current path. This may be called, say, when an array is specified as a value.
   */
  Optional<FieldValueHandler> arrayFieldValueHandler();

  /**
   * Get a {@link DocumentHandler} that is configured to handle a sub-document element at the
   * current path. This may be called, say, when a document is specified as a value.
   */
  Optional<DocumentHandler> subDocumentHandler();
}
