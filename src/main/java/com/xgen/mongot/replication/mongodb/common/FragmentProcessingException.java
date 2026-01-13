package com.xgen.mongot.replication.mongodb.common;

/**
 * Exception thrown when fragment processing encounters an error during change stream reassembly.
 * Designed to trigger non-resumable behavior that forces initial sync when thrown during change
 * stream processing.
 */
public class FragmentProcessingException extends Exception {

  public FragmentProcessingException(String message) {
    super(message);
  }

  public FragmentProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
