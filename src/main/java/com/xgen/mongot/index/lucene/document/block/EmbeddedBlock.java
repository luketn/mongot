package com.xgen.mongot.index.lucene.document.block;

import com.xgen.mongot.index.lucene.document.single.AbstractDocumentWrapper;
import com.xgen.mongot.index.lucene.document.single.IndexableFieldFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.bson.BsonDocument;

/**
 * A {@link DocumentBlock} whose parent document is not a "root", top-level Lucene document. A
 * {@link DocumentBlock} may be a child of the {@link RootBlock}, or may be a child of another
 * {@link EmbeddedBlock}.
 */
class EmbeddedBlock extends DocumentBlock {
  final Supplier<Optional<BsonDocument>> storedSourceGetter;
  private final AbstractDocumentWrapper embeddedDocument;

  EmbeddedBlock(
      AbstractDocumentWrapper document,
      List<DocumentBlock> childBlocks,
      Supplier<Optional<BsonDocument>> storedSourceGetter) {
    super(childBlocks);
    this.storedSourceGetter = storedSourceGetter;
    this.embeddedDocument = document;
  }

  static EmbeddedBlock create(
      AbstractDocumentWrapper document, Supplier<Optional<BsonDocument>> storedSourceGetter) {
    return new EmbeddedBlock(document, new ArrayList<>(), storedSourceGetter);
  }

  @Override
  Document buildRoot() {
    this.storedSourceGetter
        .get()
        .ifPresent(
            stored -> IndexableFieldFactory.addStoredSourceField(this.embeddedDocument, stored));

    return this.embeddedDocument.luceneDocument;
  }
}
