package com.xgen.mongot.index.lucene.document.single;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.index.ingestion.handlers.FieldValueHandler;
import com.xgen.mongot.index.lucene.document.builder.DocumentBlockBuilder;
import com.xgen.mongot.index.lucene.document.builder.DocumentBuilder;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.Vector;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.Document;

/**
 * {@link LuceneVectorIndexDocumentBuilder} is a {@link
 * com.xgen.mongot.index.ingestion.handlers.DocumentHandler} responsible for creating {@link
 * FieldValueHandler}s for fields at each level of an indexed document in a vector index, including
 * the root. {@link LuceneVectorIndexDocumentBuilder} is configured with:
 *
 * <ul>
 *   <li>The {@link VectorIndexFieldMapping} (e.g. mappings) for a particular vector index. *
 *   <li>A {@link VectorIndexDocumentWrapper} that contains a Lucene vector document: *
 *       <ul>
 *         <li>With a {@code $meta/_id} field. All documents contain a field with the encoded _id of
 *             * the MongoDB source document they originated from. *
 *       </ul>
 * </ul>
 */
public class LuceneVectorIndexDocumentBuilder implements DocumentBuilder, DocumentBlockBuilder {

  final VectorIndexDocumentWrapper documentWrapper;
  VectorIndexFieldMapping mapping;
  final Optional<FieldPath> path;
  final ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings;

  public LuceneVectorIndexDocumentBuilder(
      VectorIndexDocumentWrapper documentWrapper,
      VectorIndexFieldMapping mapping,
      Optional<FieldPath> path,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    this.documentWrapper = documentWrapper;
    this.mapping = mapping;
    this.path = path;
    this.autoEmbeddings = autoEmbeddings;
  }

  public static LuceneVectorIndexDocumentBuilder create(
      VectorIndexDocumentWrapper documentWrapper,
      VectorIndexFieldMapping mapping,
      Optional<FieldPath> path,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return new LuceneVectorIndexDocumentBuilder(documentWrapper, mapping, path, autoEmbeddings);
  }

  /**
   * Create a {@link LuceneVectorIndexDocumentBuilder} for a standalone Lucene document to be
   * indexed. Configures the to-be-built Lucene document with the correct _id field. Embedded fields
   * are not supported by vector indexes yet.
   */
  public static LuceneVectorIndexDocumentBuilder createRoot(
      byte[] id,
      VectorIndexFieldMapping mapping,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater,
      ImmutableMap<FieldPath, ImmutableMap<String, Vector>> autoEmbeddings) {
    return new LuceneVectorIndexDocumentBuilder(
        VectorIndexDocumentWrapper.createRoot(id, indexCapabilities, indexingMetricsUpdater),
        mapping,
        Optional.empty(),
        autoEmbeddings);
  }

  @Override
  public Optional<FieldValueHandler> valueHandler(String leafPath) {
    FieldPath fullPath = childPath(leafPath);
    return this.mapping.childPathExists(fullPath)
        ? Optional.of(
            LuceneVectorIndexFieldValueHandler.create(
                this.documentWrapper, this.mapping, fullPath, this.autoEmbeddings))
        : Optional.empty();
  }

  private FieldPath childPath(String leafPath) {
    return this.path
        .map(path -> path.newChild(leafPath))
        .orElseGet(() -> FieldPath.newRoot(leafPath));
  }

  @Override
  public Document build() throws IOException {
    return this.documentWrapper.luceneDocument;
  }

  @Override
  public List<Document> buildBlock() throws IOException {
    return List.of(build());
  }
}
