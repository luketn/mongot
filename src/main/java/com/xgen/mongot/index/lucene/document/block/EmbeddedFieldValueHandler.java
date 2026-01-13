package com.xgen.mongot.index.lucene.document.block;

import com.mongodb.client.model.geojson.Geometry;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.StoredSourceDefinition;
import com.xgen.mongot.index.ingestion.handlers.Composite;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.ingestion.stored.StoredBuilder;
import com.xgen.mongot.index.ingestion.stored.StoredDocumentBuilder;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.bson.BsonValue;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * Delegates consuming indexed values to a wrapped {@link FieldValueHandler}. Determines when to
 * create new {@link EmbeddedDocumentBuilder}s as configured by the FieldDefinition at a path.
 *
 * <p>Builds a {@link Composite.CompositeDocumentHandler} composed of itself and a new {@link
 * EmbeddedDocumentBuilder} when a new embedded document should be created. See {@link
 * #subDocumentHandler()} to see how this happens.
 */
class EmbeddedFieldValueHandler implements FieldValueHandler {
  private final Optional<FieldValueHandler> wrappedFieldValueHandler;
  private final DocumentBlock documentBlock;
  private final FieldDefinition fieldDefinition;

  private final Analyzer indexAnalyzer;
  private final SearchFieldDefinitionResolver resolver;
  private final FieldPath path;
  private final byte[] id;
  private final IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater;

  private EmbeddedFieldValueHandler(
      Optional<FieldValueHandler> wrappedFieldValueHandler,
      DocumentBlock documentBlock,
      FieldDefinition fieldDefinition,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      FieldPath path,
      byte[] id,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    this.wrappedFieldValueHandler = wrappedFieldValueHandler;
    this.documentBlock = documentBlock;
    this.fieldDefinition = fieldDefinition;
    this.indexAnalyzer = indexAnalyzer;
    this.resolver = resolver;
    this.path = path;
    this.id = id;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
  }

  static Optional<FieldValueHandler> create(
      Optional<FieldValueHandler> wrappedValueHandler,
      DocumentBlock documentBlock,
      FieldDefinition fieldDefinition,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      FieldPath path,
      byte[] id,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    return Optional.of(
        new EmbeddedFieldValueHandler(
            wrappedValueHandler,
            documentBlock,
            fieldDefinition,
            indexAnalyzer,
            resolver,
            path,
            id,
            indexingMetricsUpdater));
  }

  @Override
  public void handleBinary(Supplier<Binary> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleBinary(supplier));
  }

  @Override
  public void handleBoolean(Supplier<Boolean> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleBoolean(supplier));
  }

  @Override
  public void handleDateTime(Supplier<Long> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleDateTime(supplier));
  }

  @Override
  public void handleDouble(Supplier<Double> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleDouble(supplier));
  }

  @Override
  public void handleGeometry(Supplier<Optional<Geometry>> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleGeometry(supplier));
  }

  @Override
  public void handleInt32(Supplier<Integer> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleInt32(supplier));
  }

  @Override
  public void handleInt64(Supplier<Long> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleInt64(supplier));
  }

  @Override
  public void handleKnnVector(Supplier<Optional<Vector>> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleKnnVector(supplier));
  }

  @Override
  public void handleNull() {
    this.wrappedFieldValueHandler.ifPresent(FieldValueHandler::handleNull);
  }

  @Override
  public void handleObjectId(Supplier<ObjectId> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleObjectId(supplier));
  }

  @Override
  public void handleString(Supplier<String> supplier) throws IOException {
    if (this.wrappedFieldValueHandler.isPresent()) {
      this.wrappedFieldValueHandler.get().handleString(supplier);
    }
  }

  @Override
  public void handleUuid(Supplier<Optional<UUID>> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleUuid(supplier));
  }

  @Override
  public void handleRawBsonValue(Supplier<BsonValue> supplier) {
    this.wrappedFieldValueHandler.ifPresent(handler -> handler.handleRawBsonValue(supplier));
  }

  @Override
  public void markFieldNameExists() {
    this.wrappedFieldValueHandler.ifPresent(FieldValueHandler::markFieldNameExists);
  }

  @Override
  public Optional<FieldValueHandler> arrayFieldValueHandler() {
    return EmbeddedFieldValueHandler.create(
        this.wrappedFieldValueHandler.flatMap(FieldValueHandler::arrayFieldValueHandler),
        this.documentBlock,
        this.fieldDefinition,
        this.indexAnalyzer,
        this.resolver,
        this.path,
        this.id,
        this.indexingMetricsUpdater);
  }

  /**
   * Creates a sub document handler for a field at this path.
   *
   * <p>If the field is configured to create embedded documents, create a new {@link
   * EmbeddedDocumentBuilder} to build the new embedded document, and return a {@link
   * Composite.CompositeDocumentHandler} composed of the sub document handler for this current
   * {@link EmbeddedFieldValueHandler} and the new {@link EmbeddedDocumentBuilder}.
   */
  @Override
  public Optional<DocumentHandler> subDocumentHandler() {
    Optional<DocumentHandler> wrappedHandler = wrappedSubDocumentHandler();
    Optional<StoredSourceDefinition> storedSourceDefinition =
        this.fieldDefinition
            .embeddedDocumentsFieldDefinition()
            .flatMap(EmbeddedDocumentsFieldDefinition::storedSourceDefinition);

    return this.fieldDefinition
        .embeddedDocumentsFieldDefinition()
        .map(
            // wraps embedded document handler with regular handler, if embedded is defined
            embeddedDefinition ->
                Composite.CompositeDocumentHandler.of(
                    Optional.of(
                        EmbeddedDocumentBuilder.create(
                            this.documentBlock,
                            embeddedDefinition,
                            this.indexAnalyzer,
                            this.resolver,
                            this.id,
                            this.path,
                            this.indexingMetricsUpdater)),
                    wrappedHandler))
        .map(
            // wraps stored source handler into embedded document handler, if embedded stored source
            // is defined. Return previous handler if not.
            c ->
                storedSourceDefinition.isPresent()
                    ? Composite.CompositeDocumentHandler.of(
                        c,
                        StoredDocumentBuilder.create(storedSourceDefinition.get())
                            .map(StoredBuilder::asDocumentHandler))
                    : c)
        // returns normal handler if not embedded
        .orElse(wrappedHandler);
  }

  /**
   * Get the sub-{@link DocumentHandler} for the wrapped {@link FieldValueHandler}.
   *
   * <p>If there is a statically-defined DocumentFieldDefinition, there may be an
   * EmbeddedDocumentsFieldDefinition defined somewhere in this sub-document. In this case, return
   * the sub-{@link DocumentHandler} from the wrapped {@link FieldValueHandler} in an {@link
   * EmbeddedDocumentHandler} to keep looking for embedded documents as we descend this subtree.
   */
  private Optional<DocumentHandler> wrappedSubDocumentHandler() {
    Optional<DocumentHandler> wrappedSubDocumentHandler =
        this.wrappedFieldValueHandler.flatMap(FieldValueHandler::subDocumentHandler);

    if (this.fieldDefinition.documentFieldDefinition().isPresent()) {
      return wrappedSubDocumentHandler.map(
          documentHandler ->
              EmbeddedDocumentHandler.create(
                  documentHandler,
                  this.fieldDefinition.documentFieldDefinition().get(),
                  this.indexAnalyzer,
                  this.resolver,
                  this.documentBlock,
                  this.path,
                  this.id,
                  this.indexingMetricsUpdater));
    } else {
      return wrappedSubDocumentHandler;
    }
  }
}
