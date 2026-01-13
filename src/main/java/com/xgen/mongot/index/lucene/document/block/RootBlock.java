package com.xgen.mongot.index.lucene.document.block;

import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;

/**
 * A {@link DocumentBlock} representing the root, top-level Lucene document and any child {@link
 * EmbeddedBlock} embedded document blocks.
 */
class RootBlock extends DocumentBlock {
  final DocumentBuilder luceneRootDocBuilder;

  RootBlock(DocumentBuilder luceneRootDocBuilder, List<DocumentBlock> childBlocks) {
    super(childBlocks);
    this.luceneRootDocBuilder = luceneRootDocBuilder;
  }

  static DocumentBlock create(DocumentBuilder rootDocBuilder) {
    return new RootBlock(rootDocBuilder, new ArrayList<>());
  }

  @Override
  Document buildRoot() throws IOException {
    return this.luceneRootDocBuilder.build();
  }
}
