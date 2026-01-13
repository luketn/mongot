package com.xgen.mongot.util.retry;

import net.jodah.failsafe.RetryPolicy;

public interface BackoffPolicy {
  /** Apply retry parameters specific to the policy. */
  <T> RetryPolicy<T> applyParameters(RetryPolicy<T> retryPolicy);
}
