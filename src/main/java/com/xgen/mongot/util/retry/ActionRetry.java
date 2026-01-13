package com.xgen.mongot.util.retry;

import java.util.concurrent.Callable;

public class ActionRetry {

  public static <R> R onException(
      Callable<R> action, Class<? extends Exception> expectedException, int allowedAttempts)
      throws Exception {
    try {
      return action.call();
    } catch (Exception e) {
      if (expectedException.isInstance(e) && allowedAttempts > 0) {
        return onException(action, expectedException, allowedAttempts - 1);
      }
      throw e;
    }
  }
}
