package com.xgen.mongot.embedding.mongodb.leasing;

import static org.junit.Assert.assertEquals;

import com.xgen.mongot.index.status.IndexStatus;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StatusResolutionUtilsTest {

  private final Lease.IndexDefinitionVersionStatus requestedIndexDefinitionVersionStatus;
  private final Lease.IndexDefinitionVersionStatus latestIndexDefinitionVersionStatus;
  private final IndexStatus expectedStatus;

  public StatusResolutionUtilsTest(
      Lease.IndexDefinitionVersionStatus requestedIndexDefinitionVersionStatus,
      Lease.IndexDefinitionVersionStatus latestIndexDefinitionVersionStatus,
      IndexStatus expectedStatus) {
    this.requestedIndexDefinitionVersionStatus = requestedIndexDefinitionVersionStatus;
    this.latestIndexDefinitionVersionStatus = latestIndexDefinitionVersionStatus;
    this.expectedStatus = expectedStatus;
  }

  @Parameterized.Parameters(
      name = "Test {index}: requestedVersion{0}, latestVersion{1}, expectedStatus{2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          // Latest gen is failed, so overall status is failed.
          {
            new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY),
            new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.FAILED),
            IndexStatus.failed("Index failed.")
          },
          // Latest gen is in initial sync, but requested gen is queryable, so overall status is
          // recovering transient.
          {
            new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY),
            new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC),
            IndexStatus.recoveringTransient(
                "New version is being built but old version is queryable.")
          },
          // Both gens are queryable, so overall status is steady.
          {
            new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY),
            new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY),
            IndexStatus.steady()
          },
          // Requested gen is steady but latest gen is RECOVERING_TRANSIENT, so requested gen status
          // is returned.
          {
            new Lease.IndexDefinitionVersionStatus(true, IndexStatus.StatusCode.STEADY),
            new Lease.IndexDefinitionVersionStatus(
                true, IndexStatus.StatusCode.RECOVERING_TRANSIENT),
            IndexStatus.steady()
          }
        });
  }

  @Test
  public void testGetEffectiveMaterializedViewStatus() {
    assertEquals(
        this.expectedStatus,
        StatusResolutionUtils.getEffectiveMaterializedViewStatus(
            this.requestedIndexDefinitionVersionStatus, this.latestIndexDefinitionVersionStatus));
  }
}
