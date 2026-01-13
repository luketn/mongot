package com.xgen.mongot.index.lucene.query.pushdown.project;

import java.io.IOException;
import org.bson.BsonDocument;

/**
 * Subclasses of this interface implement a strategy for computing a projection of a single document
 * given a lucene doc ID. Strategies may include decisions such as DocValues vs StoredFields, using
 * per-type columns vs fetching full bson blobs, etc. Strategies may be selected depending on the
 * characteristics of the {@link ProjectSpec} and may delegate to other strategies on a per-segment
 * or per-document basis.
 *
 * @param <T> One of BsonDocument or RawBsonDocument. If BsonDocument, the implementation should
 *     guarantee that {@code project(id).getClass() == BsonDocument.class}. This assumption allows
 *     efficient type-safe transforms of the results.
 */
interface ProjectStrategy<T extends BsonDocument> {

  /**
   * Reads a subset of fields for a given document. It is the caller's responsibility to ensure any
   * underlying iterators are in a valid state before calling this method (e.g. if the strategy
   * relies on docValues, this method must be called with monotonically increasing docIds)
   */
  T project(int docId) throws IOException;

  /** Returns a new {@link ProjectStrategy} with resulting transform applied. */
  default <R extends BsonDocument> ProjectStrategy<R> map(ProjectionTransform<T, R> mapper) {
    return (int docId) -> mapper.project(this.project(docId));
  }
}
