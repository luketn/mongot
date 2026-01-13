package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

/**
 * A {@link DocumentWrapper} extends {@link AbstractDocumentWrapper} and is a container that
 * contains a Lucene {@code Document} and other information used for creating indexable fields which
 * are inserted into the Lucene Document.
 *
 * <p>When making choices about if a value type should be deserialized, it is not important if a
 * field is embedded or not - the configuring FieldDefinition and if the field is multi-valued (part
 * of an array or an array of documents) is the only information needed to make that determination.
 *
 * <p>Grouping the Lucene Document with the embedded root path of that document lets {@link
 * com.xgen.mongot.index.ingestion.handlers.DocumentHandler}s and {@link
 * com.xgen.mongot.index.ingestion.handlers.FieldValueHandler}s operate without knowledge of whether
 * the Lucene document they are building is part of an embedded document or not, and gives them an
 * easy way to delegate that information to places that create indexable fields and insert them into
 * documents.
 */
public class DocumentWrapper extends AbstractDocumentWrapper {

  /**
   * This value is true iff the `indexCapabilities` support sortable number and dates in
   * embeddedDocuments, or this is not an embeddedDocument.
   */
  public final boolean isNumberAndDateSortable;

  public final Analyzer indexAnalyzer;
  public final SearchFieldDefinitionResolver resolver;

  private final Optional<FieldPath> embeddedRoot;

  DocumentWrapper(
      Document luceneDocument,
      Optional<FieldPath> embeddedRoot,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    super(luceneDocument, resolver.indexCapabilities, indexingMetricsUpdater);
    this.embeddedRoot = embeddedRoot;
    this.isNumberAndDateSortable =
        embeddedRoot.isEmpty() || resolver.indexCapabilities.supportsEmbeddedNumericAndDateV2();
    this.indexAnalyzer = indexAnalyzer;
    this.resolver = resolver;
  }

  /**
   * Creates a standalone document wrapper for a root document.
   *
   * @param id - the id of the document
   * @param indexAnalyzer - the index analyzer
   * @param resolver - the resolver
   * @param indexingMetricsUpdater - the indexing metrics updater
   * @return the document wrapper
   */
  public static DocumentWrapper createRootStandalone(
      byte[] id,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    Document luceneDocument = new Document();
    DocumentWrapper wrapper =
        new DocumentWrapper(
            luceneDocument, Optional.empty(), indexAnalyzer, resolver, indexingMetricsUpdater);
    IndexableFieldFactory.addDocumentIdField(
        wrapper, id, resolver.indexCapabilities.isMetaIdSortable());
    return wrapper;
  }

  /**
   * Creates a document wrapper for a root document of an embedded document.
   *
   * @param id - the id of the document
   * @param indexAnalyzer - the index analyzer
   * @param resolver - the resolver
   * @param indexingMetricsUpdater - the indexing metrics updater
   * @return the document wrapper
   */
  static DocumentWrapper createEmbeddedRoot(
      byte[] id,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    DocumentWrapper wrapper =
        createRootStandalone(id, indexAnalyzer, resolver, indexingMetricsUpdater);
    IndexableFieldFactory.addEmbeddedRootField(wrapper);
    return wrapper;
  }

  /**
   * Creates a document wrapper for an embedded document given the embedded root path.
   *
   * @param id - the id of the document
   * @param embeddedRoot - the embedded root of the document
   * @param indexAnalyzer - the index analyzer
   * @param resolver - the resolver
   * @param indexingMetricsUpdater - the indexing metrics updater
   * @return the document wrapper
   */
  static DocumentWrapper createEmbedded(
      byte[] id,
      FieldPath embeddedRoot,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    // Add the same document id to all documents in the same embedded block. The shared "_id" field
    // between these documents is what lets Atlas Search identify documents from the same source
    // document, so they can all be deleted on an update or delete. Using docvalues in not needed
    // today, but in the future we may allow sorting on embedded documents.
    Document luceneDocument = new Document();
    DocumentWrapper wrapper =
        new DocumentWrapper(
            luceneDocument,
            Optional.of(embeddedRoot),
            indexAnalyzer,
            resolver,
            indexingMetricsUpdater);
    IndexableFieldFactory.addDocumentIdField(
        wrapper, id, resolver.indexCapabilities.isMetaIdSortable());
    IndexableFieldFactory.addEmbeddedPathField(wrapper);
    return wrapper;
  }

  SearchFieldDefinitionResolver getResolver() {
    return this.resolver;
  }

  @Override
  Optional<FieldPath> getEmbeddedRoot() {
    return this.embeddedRoot;
  }

  @Override
  Optional<Analyzer> getIndexAnalyzer() {
    return Optional.of(this.indexAnalyzer);
  }
}
