package com.xgen.mongot.index.lucene.document.block;

import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;

/**
 * Similar to {@link EmbeddedDocumentBuilder}, except not at the root of a document block. Does not
 * know how to build a {@code List<Document>} document block, and always has a FieldPath.
 */
class EmbeddedDocumentHandler implements DocumentHandler {
  final DocumentHandler wrappedFieldHandler;
  final HierarchicalFieldDefinition fieldDefinition;

  final Analyzer indexAnalyzer;
  final SearchFieldDefinitionResolver resolver;
  final DocumentBlock documentBlock;
  final FieldPath fieldPath;
  final byte[] id;
  final IndexingMetricsUpdater indexingMetricsUpdater;

  private EmbeddedDocumentHandler(
      DocumentHandler wrappedFieldHandler,
      HierarchicalFieldDefinition fieldDefinition,
      IndexingMetricsUpdater indexingMetricsUpdater,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      DocumentBlock documentBlock,
      FieldPath fieldPath,
      byte[] id) {
    this.wrappedFieldHandler = wrappedFieldHandler;
    this.fieldDefinition = fieldDefinition;
    this.indexAnalyzer = indexAnalyzer;
    this.resolver = resolver;
    this.documentBlock = documentBlock;
    this.fieldPath = fieldPath;
    this.id = id;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
  }

  static DocumentHandler create(
      DocumentHandler wrappedFieldHandler,
      HierarchicalFieldDefinition fieldDefinition,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      DocumentBlock documentBlock,
      FieldPath fieldPath,
      byte[] id,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    return new EmbeddedDocumentHandler(
        wrappedFieldHandler,
        fieldDefinition,
        indexingMetricsUpdater,
        indexAnalyzer,
        resolver,
        documentBlock,
        fieldPath,
        id);
  }

  /**
   * Create a {@link FieldValueHandler} for a value at a particular leaf path.
   *
   * <p>See {@link EmbeddedDocumentHandler#valueHandler(DocumentHandler, DocumentBlock,
   * HierarchicalFieldDefinition, IndexingMetricsUpdater, Analyzer, SearchFieldDefinitionResolver,
   * FieldPath, byte[])} .
   */
  @Override
  public Optional<FieldValueHandler> valueHandler(String leafPath) {
    return valueHandler(
        this.wrappedFieldHandler,
        this.documentBlock,
        this.fieldDefinition,
        this.indexingMetricsUpdater,
        this.indexAnalyzer,
        this.resolver,
        this.fieldPath.newChild(leafPath),
        this.id);
  }

  /**
   * Create a {@link FieldValueHandler} for a field at absolute path {@code
   * fieldValueHandlerAbsolutePath}.
   *
   * <p>If there is a statically-defined field definition for that field, return the {@link
   * FieldValueHandler} created from this class's wrapped {@link DocumentHandler} wrapped in an
   * {@link EmbeddedFieldValueHandler}, because there may be some embedded field at or under this
   * field that we may want to create a new embedded document for.
   *
   * <p>If there is no statically-defined document field for this field, there cannot be an embedded
   * field defined at this field or in a subfield of a document at this field, so there we can
   * return the {@link FieldValueHandler} from the wrapped {@link DocumentHandler} without wrapping
   * it in an {@link EmbeddedFieldValueHandler}.
   */
  static Optional<FieldValueHandler> valueHandler(
      DocumentHandler documentHandler,
      DocumentBlock documentBlock,
      HierarchicalFieldDefinition fieldDefinition,
      IndexingMetricsUpdater indexingMetricsUpdater,
      Analyzer indexAnalyzer,
      SearchFieldDefinitionResolver resolver,
      FieldPath fieldValueHandlerAbsolutePath,
      byte[] id) {
    String leafPath = fieldValueHandlerAbsolutePath.getLeaf();

    return fieldDefinition
        .getField(leafPath)
        .flatMap(
            field ->
                EmbeddedFieldValueHandler.create(
                    documentHandler.valueHandler(leafPath),
                    documentBlock,
                    field,
                    indexAnalyzer,
                    resolver,
                    fieldValueHandlerAbsolutePath,
                    id,
                    indexingMetricsUpdater))
        .or(() -> documentHandler.valueHandler(leafPath));
  }
}
