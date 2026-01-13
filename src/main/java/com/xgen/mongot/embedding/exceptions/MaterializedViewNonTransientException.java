package com.xgen.mongot.embedding.exceptions;

public class MaterializedViewNonTransientException extends RuntimeException {
  public MaterializedViewNonTransientException(String message) {
    super(message);
  }

  public MaterializedViewNonTransientException(Throwable throwable) {
    super(throwable);
  }
}
