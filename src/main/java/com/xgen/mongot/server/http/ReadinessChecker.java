package com.xgen.mongot.server.http;

/**
 * Interface for checking if the server is ready to serve traffic. This is used by the health check
 * server's readiness endpoint.
 */
public interface ReadinessChecker {
  /**
   * Checks if the server is ready to serve traffic.
   *
   * @param allowFailedIndexes if true, bypass checks for failed index status
   * @return true if the server is ready, false otherwise
   */
  boolean isReady(boolean allowFailedIndexes) throws Exception;
}
