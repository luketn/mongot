package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.xgen.mongot.util.mongodb.Errors;

public class MongoExceptionUtils {

  /**
   * Returns true if the given MongoException is a retryable exception, as documented by
   * https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst#retryable-error
   *
   * <p>We don't intend to actually retry the operation inline, since application-level retry logic
   * is tricky to implement correctly
   *
   * <p>See also:
   * https://github.com/mongodb/mongo-java-driver/blob/master/driver-core/src/main/com/mongodb/internal/operation/CommandOperationHelper.java#L89-L100
   */
  public static boolean isRetryable(MongoException e) {
    if (e instanceof MongoSocketException
        || e instanceof MongoNotPrimaryException
        || e instanceof MongoNodeIsRecoveringException
        || e instanceof MongoCursorNotFoundException
        || e instanceof MongoTimeoutException) {
      return true;
    }

    return Errors.RETRYABLE_ERROR_CODES.contains(e.getCode());
  }

  /** Returns true if the given MongoException is a retryable MongoClientException. */
  public static boolean isRetryableClientException(MongoException e) {
    return e instanceof MongoConfigurationException
        || e instanceof MongoConnectionPoolClearedException
        || e instanceof MongoSecurityException
        || e instanceof MongoServerUnavailableException;
  }

  /**
   * Returns true if the given MongoException is a non-invalidating resync-able exception. See
   * {@link Errors#NON_INVALIDATING_ERROR_CODES} for exact list of errors.
   */
  public static boolean isNonInvalidatingResyncable(MongoException e) {
    return Errors.NON_INVALIDATING_ERROR_CODES.contains(e.getCode());
  }
}
