package com.xgen.mongot.replication.mongodb.common;

import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

/**
 * CollectionScanMongoClient exposes all the functionality required from MongoDB to perform
 * collection scan queries and get the results in batches.
 */
public interface CollectionScanMongoClient<E extends Exception> extends AutoCloseable {

  /** Returns the next batch for this collection scan. */
  List<RawBsonDocument> getNext() throws E;

  /**
   * Returns an opTime greater than or equal to the opTime of the final getMore in the cursor
   *
   * <p>Must be called after exhausting the cursor.
   */
  BsonTimestamp getMinValidOpTime() throws E;

  /**
   * Returns the operationTime that this find command began at. Empty before the cursor has been
   * opened.
   */
  BsonTimestamp getOperationTime() throws E;

  /**
   * Returns true if client.state is OPEN_CURSOR or GET_MORE. If state is OPEN_CURSOR, executes the
   * client's NamespaceChangeCheck
   */
  boolean hasNext() throws E;

  /**
   * Returns postBatchResumeToken if any, postBatchResumeToken only exists if natural order scan is
   * applied.
   */
  Optional<BsonDocument> getPostBatchResumeToken();

  @Override
  void close();
}
