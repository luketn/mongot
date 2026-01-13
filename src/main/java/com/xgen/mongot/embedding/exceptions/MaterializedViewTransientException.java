package com.xgen.mongot.embedding.exceptions;

public class MaterializedViewTransientException extends RuntimeException {
  public MaterializedViewTransientException(String message) {
    super(message);
  }

  public MaterializedViewTransientException(Throwable throwable) {
    super(throwable);
  }
}
