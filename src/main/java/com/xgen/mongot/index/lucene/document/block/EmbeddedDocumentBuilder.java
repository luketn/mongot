package com.xgen.mongot.index.lucene.document.block;

import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.ingestion.handlers.Composite;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.ingestion.stored.StoredBuilder;
import com.xgen.mongot.index.ingestion.stored.StoredDocumentBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.lucene.document.single.ExistingDocumentWrapper;
import com.xgen.mongot.index.lucene.document.single.LuceneSearchIndexDocumentBuilder;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

/**
 * {@link EmbeddedDocumentBuilder} contains a wrapped {@link DocumentBuilder}, and creates {@link
 * EmbeddedFieldValueHandler}s. {@link EmbeddedFieldValueHandler} and {@link
 * EmbeddedDocumentHandler} delegate most indexing work to their wrapped {@link DocumentHandler} and
 * {@link FieldValueHandler}s.
 *
 * <p>The main responsibility of {@link EmbeddedDocumentBuilder} is to check if a new embedded
 * document should be created at a path. See {@link EmbeddedFieldValueHandler#subDocumentHandler()}
 * to see how creation of a new embedded document builder happens.
 */
public class EmbeddedDocumentBuilder implements DocumentBlockBuilder {
  final DocumentHandler wrappedDocumentBuilder;
  final DocumentBlock documentBlock;
  final HierarchicalFieldDefinition fieldDefinition;

  final Analyzer indexAnalyzer;
  final SearchFieldDefinitionResolver resolver;
  final byte[] id;
  private final Optional<FieldPath> path;
  final IndexingMetricsUpdater indexingMetricsUpdater;

  private EmbeddedDocumentBuilder(
      DocumentHandler wrappedDocumentBuilder,
      DocumentBlock documentBlock,
      HierarchicalFieldDefinition fieldDefinition,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      byte[] id,
      Optional<FieldPath> path,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    if (fieldDefinition instanceof EmbeddedDocumentsFieldDefinition embeddedDocumentsFieldDefinition
        && embeddedDocumentsFieldDefinition.storedSourceDefinition().isPresent()) {
      this.wrappedDocumentBuilder =
          Check.isPresent(
              Composite.CompositeDocumentHandler.of(
                  Optional.of(wrappedDocumentBuilder),
                  StoredDocumentBuilder.create(
                          embeddedDocumentsFieldDefinition.storedSourceDefinition().get())
                      .map(StoredBuilder::asDocumentHandler)),
              "wrappedDocumentBuilder");
    } else {
      this.wrappedDocumentBuilder = wrappedDocumentBuilder;
    }
    this.documentBlock = documentBlock;
    this.fieldDefinition = fieldDefinition;
    this.indexAnalyzer = indexAnalyzer;
    this.resolver = resolver;
    this.id = id;
    this.path = path;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
  }

  public static EmbeddedDocumentBuilder createRoot(
      DocumentBuilder wrappedDocumentBuilder,
      DocumentFieldDefinition mappings,
      IndexingMetricsUpdater indexingMetricsUpdater,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      byte[] id) {
    return new EmbeddedDocumentBuilder(
        wrappedDocumentBuilder,
        RootBlock.create(wrappedDocumentBuilder),
        mappings,
        indexAnalyzer,
        resolver,
        id,
        Optional.empty(),
        indexingMetricsUpdater);
  }

  static EmbeddedDocumentBuilder create(
      DocumentBlock parentBlock,
      EmbeddedDocumentsFieldDefinition embeddedDefinition,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      byte[] id,
      FieldPath path,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    LuceneSearchIndexDocumentBuilder documentBuilder =
        LuceneSearchIndexDocumentBuilder.createEmbedded(
            id, path, embeddedDefinition, indexAnalyzer, resolver, indexingMetricsUpdater);

    var stored = embeddedDefinition.storedSourceDefinition().flatMap(StoredDocumentBuilder::create);
    var composite =
        stored
            .flatMap(
                storedDocumentBuilder ->
                    Composite.CompositeDocumentHandler.of(
                        Optional.of(documentBuilder),
                        Optional.of(storedDocumentBuilder.asDocumentHandler())))
            .orElse(documentBuilder);

    var backingDocument =
        ExistingDocumentWrapper.create(
            documentBuilder.backingDocument(), resolver.indexCapabilities, indexingMetricsUpdater);

    return new EmbeddedDocumentBuilder(
        composite,
        parentBlock.newChild(backingDocument, () -> stored.flatMap(StoredBuilder::build)),
        embeddedDefinition,
        indexAnalyzer,
        resolver,
        id,
        Optional.of(path),
        indexingMetricsUpdater);
  }

  /**
   * Create a {@link FieldValueHandler} for a value at a particular leaf path.
   *
   * <p>See {@link EmbeddedDocumentHandler#valueHandler(DocumentHandler, DocumentBlock,
   * HierarchicalFieldDefinition, IndexingMetricsUpdater , Analyzer, SearchFieldDefinitionResolver,
   * FieldPath, byte[])}}.
   */
  @Override
  public Optional<FieldValueHandler> valueHandler(String leafPath) {
    return EmbeddedDocumentHandler.valueHandler(
        this.wrappedDocumentBuilder,
        this.documentBlock,
        this.fieldDefinition,
        this.indexingMetricsUpdater,
        this.indexAnalyzer,
        this.resolver,
        this.path.map(path -> path.newChild(leafPath)).orElseGet(() -> FieldPath.newRoot(leafPath)),
        this.id);
  }

  @Override
  public List<Document> buildBlock() throws IOException {
    return this.documentBlock.build();
  }
}
