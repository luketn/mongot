package com.xgen.mongot.embedding.mongodb.leasing;

import com.xgen.mongot.index.status.IndexStatus;

/**
 * Utility class containing methods for resolving the effective status of an auto-embedding related
 * entity based on the status of its constituent parts.
 */
public class StatusResolutionUtils {

  public static IndexStatus getEffectiveMaterializedViewStatus(
      Lease.IndexDefinitionVersionStatus requestedIndexDefinitionVersionStatus,
      Lease.IndexDefinitionVersionStatus latestIndexDefinitionVersionStatus) {
    // There are a few scenarios we need to handle here before returning the status. Here, we assume
    // the latest index definition version is the staged version in the case of the requested and
    // latest being different.
    // Scenario 1: There are two versions, one live and one staged. Staged one gets into failed
    // status, we want to return failed status for both versions.
    if (latestIndexDefinitionVersionStatus.indexStatusCode() == IndexStatus.StatusCode.FAILED) {
      return IndexStatus.failed("Index failed.");
    }
    // Scenario 2: There are two versions, one live and one staged. Live one reached queryable
    // state but staged one is still being built. In this case, we want to return
    // RECOVERING_TRANSIENT for the old one to indicate that the version is queryable but might
    // be stale and at the same time we don't want the config manager to initiate a new attempt. We
    // return the actual status for the staged one.
    if (requestedIndexDefinitionVersionStatus.isQueryable()
        && !latestIndexDefinitionVersionStatus.isQueryable()) {
      return IndexStatus.recoveringTransient(
          "New version is being built but old version is queryable.");
    }
    // Scenario 3: In all other cases, we return the status of the requested version.
    return new IndexStatus(requestedIndexDefinitionVersionStatus.indexStatusCode());
  }
}
