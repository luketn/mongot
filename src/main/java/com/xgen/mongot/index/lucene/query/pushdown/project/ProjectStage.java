package com.xgen.mongot.index.lucene.query.pushdown.project;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * A ProjectStage encapsulates logic to fetch _id and compute requested projections for a given
 * docId. This class is not thread safe and should be instantiated per batch of search results.
 */
@NotThreadSafe
public interface ProjectStage {

  /**
   * Returns the requested projection of document with Lucene docID=`docId`.
   *
   * <p>Creates a new {@link RawBsonDocument} which contains a subset of fields of the document. The
   * order in which the fields appear in the resulting document must be in either the original
   * document order or alphabetical order. If path matches a sub-document, the whole sub-document is
   * returned. If path matches a BsonArray, the entire array is returned. If the document contains
   * duplicate keys, the result is undefined.
   *
   * <p>If no $project stage eligible for pushdown was specified, then this method returns {@code
   * Optional.empty()}. This method should always be called by the same thread that instantiated the
   * {@code ProjectStage}.
   *
   * <p>The presence of {@code project(int)} and {@link #getId(int)} are mutually exclusive. A given
   * {@code ProjectStage} will either return {@link Optional#empty()} for all projections or for all
   * IDs. Exactly one value is guaranteed. In JDK21, this could return a sealed class, but since
   * this is invoked once per hit, we prefer avoiding wrapping the result in another object.
   */
  Optional<BsonDocument> project(int docId) throws IOException;

  /**
   * Returns the _id of the document with the given Lucene `docId` if and only if
   * $project/returnStoredSource was not requested.
   *
   * <p>This method should always be called by the same thread that instantiated the {@code
   * ProjectStage}.
   *
   * <p>The presence of {@link #project(int)} and {@code getId(int)} are mutually exclusive. A given
   * {@code ProjectStage} will either return {@link Optional#empty()} for all projections or for all
   * IDs. Exactly one value is guaranteed. In JDK21, this could return a sealed class, but since
   * this is invoked once per hit, we prefer avoiding wrapping the result in another object.
   *
   * @throws AssertionError if an _id value is expected but not found
   */
  Optional<BsonValue> getId(int docId) throws IOException;
}
