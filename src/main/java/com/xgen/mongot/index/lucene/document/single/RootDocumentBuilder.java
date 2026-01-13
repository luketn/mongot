package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.ingestion.handlers.Composite;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.ingestion.stored.StoredBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * Is responsible for creating a composite {@link DocumentHandler} which wraps {@link
 * DocumentHandler}s to build stored-source-bson-document and lucene-document index components side
 * by side.
 *
 * <p>Is also responsible for adding stored-source bson in the appropriate place in the root Lucene
 * document, and for applying an optional FacetsConfig to a Lucene document at build-time.
 */
public class RootDocumentBuilder implements DocumentBuilder, DocumentBlockBuilder {
  private final Optional<DocumentHandler> documentHandler;
  private final CheckedSupplier<Document, IOException> luceneDocumentGetter;
  private final Supplier<Optional<BsonDocument>> storedSourceGetter;
  private final Optional<FacetsConfig> facetsConfig;
  private final String indexId;
  private final IndexCapabilities indexCapabilities;
  private final IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater;

  private RootDocumentBuilder(
      ObjectId indexId,
      Optional<DocumentHandler> documentHandler,
      CheckedSupplier<Document, IOException> luceneDocumentGetter,
      Supplier<Optional<BsonDocument>> storedSourceGetter,
      Optional<FacetsConfig> facetsConfig,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    this.indexId = indexId.toString();
    this.documentHandler = documentHandler;
    this.facetsConfig = facetsConfig;
    this.luceneDocumentGetter = luceneDocumentGetter;
    this.storedSourceGetter = storedSourceGetter;
    this.indexCapabilities = indexCapabilities;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
  }

  public static RootDocumentBuilder create(
      ObjectId indexId,
      DocumentBuilder luceneDocumentBuilder,
      Optional<StoredBuilder> storedBuilder,
      Optional<FacetsConfig> facetsConfig,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    return new RootDocumentBuilder(
        indexId,
        Composite.CompositeDocumentHandler.of(
            Optional.of(luceneDocumentBuilder),
            storedBuilder.map(StoredBuilder::asDocumentHandler)),
        luceneDocumentBuilder::build,
        () -> storedBuilder.flatMap(StoredBuilder::build),
        facetsConfig,
        indexCapabilities,
        indexingMetricsUpdater);
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String field) {
    if (this.indexId.equals(field)) {
      // if we encounter a field named as indexId, that's our internal metadata namespace that
      // should be skipped for indexing
      return Optional.empty();
    }
    return this.documentHandler.flatMap(docHandler -> docHandler.valueHandler(field));
  }

  @Override
  public Document build() throws IOException {
    Document document = this.luceneDocumentGetter.get();
    ExistingDocumentWrapper wrapper =
        ExistingDocumentWrapper.create(
            document, this.indexCapabilities, this.indexingMetricsUpdater);

    this.storedSourceGetter
        .get()
        .ifPresent(stored -> IndexableFieldFactory.addStoredSourceField(wrapper, stored));

    if (this.facetsConfig.isPresent()) {
      return this.facetsConfig.get().build(document);
    }

    return document;
  }

  @Override
  public List<Document> buildBlock() throws IOException {
    return List.of(build());
  }
}
