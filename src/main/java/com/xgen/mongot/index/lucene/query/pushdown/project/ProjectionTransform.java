package com.xgen.mongot.index.lucene.query.pushdown.project;

import org.bson.BsonDocument;

/**
 * Instances of this interface apply a transform onto an existing projection of a document. Use
 * cases of transforms may include removing subtrees of a previous transform.
 *
 * @param <T> One of {BsonDocument, RawBsonDocument}. If BsonDocument, then the invariant {@code
 *     project(id).getClass == BsonDocument.class} should hold. This allows multiple transforms to
 *     be chained efficiently in a type-safe manner.
 */
@FunctionalInterface
interface ProjectionTransform<T extends BsonDocument, R extends BsonDocument> {

  R project(T input);
}
