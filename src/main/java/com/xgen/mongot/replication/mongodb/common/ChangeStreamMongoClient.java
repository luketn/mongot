package com.xgen.mongot.replication.mongodb.common;

/**
 * ChangeStreamMongoClient provides access to ChangeStreamBatches for a change stream on a
 * collection.
 *
 * <p>ChangeStreamMongoClient implementations are not expected to be thread safe.
 */
public interface ChangeStreamMongoClient<E extends Exception> extends AutoCloseable {

  ChangeStreamBatch getNext() throws E;

  @Override
  void close();
}
