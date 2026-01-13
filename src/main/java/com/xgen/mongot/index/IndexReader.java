package com.xgen.mongot.index;

import java.io.IOException;

/** An IndexReader is capable of reading an index and producing results for queries. */
public interface IndexReader {

  /**
   * Refreshes the IndexReader such that future calls to query() should see all updates to the Index
   * that occurred prior to the refresh() call.
   */
  void refresh() throws IOException, ReaderClosedException;

  void open();

  void close();

  /**
   * If the index read by the IndexReader is a vector index, the required memory is calculated based
   * on the vector type and the quantization type. This value is used to generate the required
   * memory metric.
   *
   * @return long value representing the required memory of the vector index in bytes
   * @throws ReaderClosedException if IndexReader is closed
   */
  long getRequiredMemoryForVectorData() throws ReaderClosedException;
}
