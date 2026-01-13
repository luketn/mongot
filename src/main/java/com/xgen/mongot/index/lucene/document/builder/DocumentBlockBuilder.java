package com.xgen.mongot.index.lucene.document.builder;

import com.xgen.mongot.index.ingestion.handlers.DocumentHandler;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.Document;

/**
 * Is able to create a {@code List<Document>} that can be inserted into a Lucene index. {@link
 * com.xgen.mongot.index.lucene.LuceneIndexWriter} is instantiated with a {@link
 * com.xgen.mongot.index.lucene.document.LuceneIndexingPolicy} that lets it create {@link
 * DocumentBlockBuilder}s for source documents.
 *
 * <p>A {@link DocumentBlockBuilder} is also a {@link DocumentHandler}, which can be passed in to
 * {@link com.xgen.mongot.index.ingestion.BsonDocumentProcessor} along with a source {@link
 * org.bson.RawBsonDocument}.
 */
public interface DocumentBlockBuilder extends DocumentHandler {
  List<Document> buildBlock() throws IOException;
}
