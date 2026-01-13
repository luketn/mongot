package com.xgen.mongot.index;

import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.util.Bytes;
import java.io.Closeable;
import java.io.IOException;
import org.bson.BsonArray;

/**
 * Provides an interface for producing batches of bson results. Users of this API might want to
 * collect some information in-between calls to <code>execute</code> and <code>getNextBatch</code>
 * (Explain queries). Note that this interface is not guaranteed to be thread safe - see detailed
 * docs on a concrete implementation.
 */
public interface BatchProducer extends Closeable {

  /**
   * Generates results and stores their state within the `BatchProducer`. This must be called
   * **before** <code>BatchProducer::getNextBatch</code>.
   */
  void execute(Bytes sizeLimit, BatchCursorOptions queryCursorOptions) throws IOException;

  /**
   * Serializes the results generated from the prior <code>BatchProducer::execute</code>. This must
   * be called **after** <code>BatchProducer::execute</code>.
   */
  BsonArray getNextBatch(Bytes resultsSizeLimit) throws IOException;

  /**
   * If true, there are no more hits to retrieve and that a getNextBatch() will return an empty
   * array.
   */
  boolean isExhausted();
}
