package com.xgen.mongot.index.lucene.query.pushdown.project;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.bson.BsonDocument;

/**
 * A ProjectionSource reads or constructs a BsonDocument by reading one or more columns from
 * StoredFields and/or DocValues. This is always a superset of the final projection.
 */
@FunctionalInterface
interface ProjectionSource<T extends BsonDocument> {

  /** Creates a {@link ProjectStrategy} for a given batch of hits. */
  ProjectStrategy<T> create(IndexReader reader) throws IOException;

  /**
   * Returns a new {@link ProjectionSource} which transforms the result of the initial projection.
   */
  default <R extends BsonDocument> ProjectionSource<R> map(ProjectionTransform<T, R> filter) {
    return r -> create(r).map(filter);
  }
}
