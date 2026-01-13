package com.xgen.mongot.catalogservice;

import com.mongodb.MongoException;
import com.xgen.mongot.replication.mongodb.common.MongoExceptionUtils;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.LoggableException;
import java.util.concurrent.Callable;

public class MetadataServiceException extends LoggableException {

  private final Type type;

  private MetadataServiceException(Type type, String message, Throwable cause) {
    super(message, cause);
    this.type = type;
  }

  public static MetadataServiceException createTransient(Throwable cause) {
    Check.isNotNull(cause.getMessage(), "message");
    return new MetadataServiceException(Type.TRANSIENT, cause.getMessage(), cause);
  }

  public static MetadataServiceException createFailed(Throwable cause) {
    Check.isNotNull(cause.getMessage(), "message");
    return new MetadataServiceException(Type.FAILED, cause.getMessage(), cause);
  }

  /**
   * Runs the supplied Callable, wrapping well-known exception types into CatalogServiceException if
   * they are caught.
   *
   * <p>Errors are propagated as is.
   */
  public static <T> T wrapIfThrows(Callable<T> callable) throws MetadataServiceException {
    try {
      return callable.call();
    } catch (MongoException e) {
      if (MongoExceptionUtils.isRetryable(e) || MongoExceptionUtils.isRetryableClientException(e)) {
        throw createTransient(e);
      } else {
        throw createFailed(e);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new AssertionError("threw unexpected checked exception: " + e, e);
    }
  }

  public Type getType() {
    return this.type;
  }

  public boolean isTransient() {
    return this.type == Type.TRANSIENT;
  }

  public boolean isFailed() {
    return this.type == Type.FAILED;
  }

  public enum Type {
    /** A transient error which may not re-occur if the operation is retried. */
    TRANSIENT,

    /** An error that will likely not resolve itself if we try again. */
    FAILED,
  }
}
