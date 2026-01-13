package com.xgen.mongot.replication.mongodb.common;

/** Exception thrown by {@link ChangeStreamMongoCursorClient} on internal validation failure. */
public class ChangeStreamCursorClientException extends Exception {

  public ChangeStreamCursorClientException(Throwable cause) {
    super(cause);
  }
}
