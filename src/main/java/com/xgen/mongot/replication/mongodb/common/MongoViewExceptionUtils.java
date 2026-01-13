package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoQueryException;
import com.xgen.mongot.index.status.RecoveringStatusReason;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoViewExceptionUtils {
  private static final Logger logger = LoggerFactory.getLogger(MongoViewExceptionUtils.class);

  /**
   * Returns true if the error is potentially related to the view pipeline not being able to work
   * with certain documents.
   */
  public static boolean isViewPipelineRelated(Optional<Throwable> cause) {

    if (cause.isEmpty()) {
      return false;
    }

    if (cause.get() instanceof MongoCursorNotFoundException) {
      // inapplicable subtype of MongoQueryException
      return false;
    }

    if (cause.get() instanceof MongoException e && MongoExceptionUtils.isRetryable(e)) {
      // errors that are retryable in general are not directly related to views
      return false;
    }

    return cause.get() instanceof MongoQueryException
        || cause.get() instanceof MongoCommandException;
  }

  public static String getViewPipelineErrorMessage(Throwable cause) {
    Check.argNotNull(cause, "cause");
    String errorMessage =
        StringUtils.defaultIfBlank(extractDriverErrorMessage(cause), "unknown error");

    return RecoveringStatusReason.VIEW_PIPELINE_ERROR.formatMessage(errorMessage);
  }

  private static String extractDriverErrorMessage(Throwable cause) {

    if (cause instanceof MongoQueryException) {
      return ((MongoQueryException) cause).getErrorMessage();
    } else if (cause instanceof MongoCommandException) {
      return ((MongoCommandException) cause).getErrorMessage();
    }

    logger.error("Unexpected exception type: {}", cause.getClass().getName());
    return Check.unreachable("Unexpected exception type");
  }
}
