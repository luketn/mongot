package com.xgen.mongot.index.lucene.document;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;

/**
 * Is able to instantiate {@link DocumentBlockBuilder}s given the {@code byte[] id} of a given
 * source document. Is the interface used externally to create a document block builder used to
 * create indexed documents for a new or updated document.
 */
public interface LuceneIndexingPolicy {
  DocumentBlockBuilder createBuilder(byte[] id);

  default DocumentBlockBuilder createBuilder(
      byte[] id, ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return createBuilder(id);
  }
}
