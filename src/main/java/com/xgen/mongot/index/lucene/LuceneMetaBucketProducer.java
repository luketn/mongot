package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.IntermediateFacetBucket;
import java.io.IOException;

/**
 * Interface for producing and consuming meta buckets. While isExhausted() is false, peek() should
 * be called to get a bucket and accept() should be called to advance to the next bucket. For
 * example: <code>
 *   while(!bucketProducer.isExhausted()) {
 *     var result = bucketProducer.peek();
 *     if(isAcceptable(result)) {
 *       bucketProducer.accept();
 *     } else {
 *       // return or break from loop and resolve the unacceptable
 *       // result before continuing to iterate over this producer
 *     }
 *   }
 * </code>
 */
public interface LuceneMetaBucketProducer {
  /**
   * Returns the current "next" bucket but does not consume it. This method will return the same
   * bucket until accept() is called to advance to the next one.
   */
  IntermediateFacetBucket peek() throws IOException;

  /** Advances to the next bucket. */
  void acceptAndAdvance();

  boolean isExhausted();
}
