package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

/**
 * Responsible for creating {@link FieldValueHandler}s for fields of an indexed document. Similar to
 * {@link LuceneSearchIndexDocumentBuilder}, but it is not at the root of a document (it has a
 * FieldPath), and it does not know how to build a Lucene document.
 */
public class LuceneDocumentHandler implements DocumentHandler {
  final DocumentWrapper documentWrapper;
  final FieldPath fieldPath;
  final HierarchicalFieldDefinition fieldDefinition;
  final boolean isMultiValued;

  private LuceneDocumentHandler(
      DocumentWrapper documentWrapper,
      FieldPath fieldPath,
      HierarchicalFieldDefinition fieldDefinition,
      boolean isMultiValued) {
    this.documentWrapper = documentWrapper;
    this.fieldPath = fieldPath;
    this.fieldDefinition = fieldDefinition;
    this.isMultiValued = isMultiValued;
  }

  static DocumentHandler create(
      DocumentWrapper documentWrapper,
      DocumentFieldDefinition fieldDefinition,
      FieldPath fieldPath,
      boolean isMultiValued) {
    return new LuceneDocumentHandler(documentWrapper, fieldPath, fieldDefinition, isMultiValued);
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String leafPath) {
    return this.fieldDefinition
        .getField(leafPath)
        .map(
            field ->
                LuceneSearchIndexFieldValueHandler.create(
                    this.documentWrapper, field, childPath(leafPath), this.isMultiValued))
        .or(
            () ->
                this.documentWrapper
                    .getResolver()
                    .getDynamicFieldDefinition(this.fieldDefinition)
                    .map(
                        dynamicFieldDef ->
                            LuceneSearchIndexFieldValueHandler.create(
                                this.documentWrapper,
                                dynamicFieldDef,
                                childPath(leafPath),
                                this.isMultiValued)));
  }

  private FieldPath childPath(String leafPath) {
    return this.fieldPath.newChild(leafPath);
  }
}
