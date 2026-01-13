package com.xgen.mongot.blobstore;

import com.xgen.mongot.util.LoggableException;

/**
 * General exception that captures any error related to the index on blobstore, and during
 * download/upload of index snapshots.
 *
 * <p>Checked exception and marked abstract to ensure we classify customer/service/unexpected errors
 * and handle them appropriately.
 */
public abstract class BlobstoreException extends LoggableException {
  public BlobstoreException(String message) {
    super(message);
  }

  public BlobstoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
