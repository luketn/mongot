package com.xgen.mongot.util;

public class RepeatableActionFailureException extends RuntimeException {

  RepeatableActionFailureException(Throwable t) {
    super(t);
  }
}
