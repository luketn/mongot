package com.xgen.mongot.index;

import com.xgen.mongot.util.LoggableException;

public class WriterClosedException extends LoggableException {
  private WriterClosedException(String message) {
    super(message);
  }

  public static WriterClosedException create(String methodName) {
    return new WriterClosedException(
        String.format("%s() is called after writer is closed", methodName));
  }
}
