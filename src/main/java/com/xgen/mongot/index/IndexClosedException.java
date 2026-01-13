package com.xgen.mongot.index;

public class IndexClosedException extends IllegalStateException {

  /**
   * A RuntimeException to use when a mongot index has been closed, and someone is trying to perform
   * an action that is not allowed on a closed index
   */
  public IndexClosedException(String message) {
    super(message);
  }

  public IndexClosedException(String message, Throwable cause) {
    super(message, cause);
  }

  public IndexClosedException(Throwable cause) {
    super(cause);
  }
}
