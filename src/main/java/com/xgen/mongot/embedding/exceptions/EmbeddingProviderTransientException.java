package com.xgen.mongot.embedding.exceptions;

public class EmbeddingProviderTransientException extends RuntimeException {
  public EmbeddingProviderTransientException(String message) {
    super(message);
  }

  public EmbeddingProviderTransientException(Throwable throwable) {
    super(throwable);
  }
}
