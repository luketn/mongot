package com.xgen.mongot.index.lucene.document.block;

import com.xgen.mongot.index.lucene.document.single.AbstractDocumentWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.bson.BsonDocument;

/**
 * {@link DocumentBlock} encapsulates a parent Lucene document and some potential embedded child
 * documents. {@link RootBlock} describes how the outer-most, root Lucene document block behaves and
 * is built. {@link EmbeddedBlock} describes a "layer" of embedding - one parent document and a
 * number of potential embedded child blocks.
 */
abstract class DocumentBlock {
  final List<DocumentBlock> childBlocks;

  DocumentBlock(List<DocumentBlock> childBlocks) {
    this.childBlocks = childBlocks;
  }

  /**
   * When an {@link EmbeddedFieldValueHandler} creates a new {@link EmbeddedDocumentBuilder} and
   * begins building a new embedded child document, it creates a new {@link DocumentBlock} for that
   * in-progress embedded document wrapped in an {@link AbstractDocumentWrapper} and inserts that
   * nascent {@link DocumentBlock} into its parent.
   *
   * <p>Inserting the in-progress document block into the parent at creation time frees the new
   * child block from having to insert itself in its parent later, after it is complete. A child
   * document block does not need to do anything to "close" or "finish" itself when it is done being
   * populated.
   */
  DocumentBlock newChild(
      AbstractDocumentWrapper childDocument, Supplier<Optional<BsonDocument>> storedSourceGetter) {
    EmbeddedBlock newBlock = EmbeddedBlock.create(childDocument, storedSourceGetter);
    this.childBlocks.add(newBlock);
    return newBlock;
  }

  List<Document> build() throws IOException {
    ArrayList<Document> block = new ArrayList<>();
    for (var childBlock : this.childBlocks) {
      block.addAll(childBlock.build());
    }
    block.add(buildRoot());
    return block;
  }

  abstract Document buildRoot() throws IOException;
}
