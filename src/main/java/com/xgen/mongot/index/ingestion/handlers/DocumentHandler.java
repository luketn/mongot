package com.xgen.mongot.index.ingestion.handlers;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import java.util.Optional;

/**
 * When {@link DocumentHandler} and {@link FieldValueHandler}s create other {@link DocumentHandler}
 * and {@link FieldValueHandler}s for subfields, sub documents, and sub arrays, those "child"
 * handlers are instantiated with immutable state.
 *
 * <p>That state, for {@link com.xgen.mongot.index.lucene.document.single.LuceneDocumentHandler},
 * includes the {@link DocumentFieldDefinition} or {@link EmbeddedDocumentsFieldDefinition} of the
 * index at that particular part of the source document.
 *
 * <p>For {@link com.xgen.mongot.index.ingestion.stored.StoredDocumentHandler}, that includes things
 * like a {@link com.xgen.mongot.index.definition.StoredSourceDefinition}, and the absolute path of
 * the source document.
 */
public interface DocumentHandler {
  Optional<FieldValueHandler> valueHandler(String field);
}
