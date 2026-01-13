package com.xgen.mongot.index.lucene.document.single;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.version.IndexCapabilities;
import org.apache.lucene.document.Document;

/**
 * A {@link VectorIndexDocumentWrapper} extends {@link AbstractDocumentWrapper} and is a container
 * that contains a Lucene {@code Document} and other information used for creating indexable fields
 * for vector indexes.
 */
public class VectorIndexDocumentWrapper extends AbstractDocumentWrapper {

  VectorIndexDocumentWrapper(
      Document luceneDocument,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    super(luceneDocument, indexCapabilities, indexingMetricsUpdater);
  }

  public static VectorIndexDocumentWrapper createRoot(
      byte[] id,
      IndexCapabilities indexCapabilities,
      IndexMetricsUpdater.IndexingMetricsUpdater indexingMetricsUpdater) {
    Document luceneDocument = new Document();
    VectorIndexDocumentWrapper wrapper =
        new VectorIndexDocumentWrapper(luceneDocument, indexCapabilities, indexingMetricsUpdater);
    IndexableFieldFactory.addDocumentIdField(wrapper, id, false);
    return wrapper;
  }
}
