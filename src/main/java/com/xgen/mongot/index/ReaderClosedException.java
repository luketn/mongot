package com.xgen.mongot.index;

import com.xgen.mongot.util.LoggableException;

public class ReaderClosedException extends LoggableException {
  private ReaderClosedException(String message) {
    super(message);
  }

  public static ReaderClosedException create(String methodName) {
    return new ReaderClosedException(
        String.format("%s() is called after reader is closed", methodName));
  }
}
