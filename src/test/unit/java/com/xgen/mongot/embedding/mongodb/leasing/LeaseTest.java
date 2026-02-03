package com.xgen.mongot.embedding.mongodb.leasing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.BsonUtils;
import com.xgen.testing.mongot.replication.mongodb.ChangeStreamUtils;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonTimestamp;
import org.junit.Test;

/** Unit tests for {@link Lease}. */
public class LeaseTest {

  private static final String LEASE_ID = "test-lease-id";
  private static final String COLLECTION_UUID = UUID.randomUUID().toString();
  private static final String COLLECTION_NAME = "test-collection";
  private static final String LEASE_OWNER = "localhost";

  @Test
  public void testExtractHighWaterMarkFromSteadyState() {
    // Set up a lease with steady state commitInfo containing a resume token
    var opTime = new BsonTimestamp(9876543210L);
    var resumeToken = ChangeStreamUtils.resumeToken(opTime);
    var indexCommitUserData =
        IndexCommitUserData.createChangeStreamResume(
            ChangeStreamResumeInfo.create(new MongoNamespace("test", "collection"), resumeToken),
            IndexFormatVersion.CURRENT);

    var lease = createLeaseWithCommitInfo(indexCommitUserData.toEncodedData().asString());

    var extractedHighWaterMark = lease.extractHighWaterMark();

    assertTrue(extractedHighWaterMark.isPresent());
    assertEquals(opTime, extractedHighWaterMark.get());
  }

  @Test
  public void testExtractHighWaterMarkFromEmptyCommitInfo() {
    // Lease with empty commitInfo (V1 hasn't started yet)
    var lease = createLeaseWithCommitInfo(EncodedUserData.EMPTY.asString());

    var extractedHighWaterMark = lease.extractHighWaterMark();

    assertFalse(extractedHighWaterMark.isPresent());
  }

  @Test
  public void testWithNewIndexDefinitionVersionPreservesHighWaterMarkFromSteadyState() {
    // Set up a lease with steady state commitInfo
    var opTime = new BsonTimestamp(9876543210L);
    var resumeToken = ChangeStreamUtils.resumeToken(opTime);
    var indexCommitUserData =
        IndexCommitUserData.createChangeStreamResume(
            ChangeStreamResumeInfo.create(new MongoNamespace("test", "collection"), resumeToken),
            IndexFormatVersion.CURRENT);

    var lease = createLeaseWithCommitInfo(indexCommitUserData.toEncodedData().asString());

    // Create V2 lease
    var v2Lease = lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync());

    // Parse V2's commitInfo and verify it contains the preserved highWaterMark
    var encodedUserData = EncodedUserData.fromString(v2Lease.commitInfo());
    var userData = IndexCommitUserData.fromEncodedData(encodedUserData, Optional.empty());

    assertTrue(userData.getInitialSyncResumeInfo().isPresent());
    assertEquals(opTime, userData.getInitialSyncResumeInfo().get().getResumeOperationTime());
    // Verify scan position is reset to MIN_KEY
    assertEquals(BsonUtils.MIN_KEY, userData.getInitialSyncResumeInfo().get().getResumeToken());
  }

  @Test
  public void testWithNewIndexDefinitionVersionWithEmptyCommitInfo() {
    // Lease with empty commitInfo (V1 hasn't started yet)
    var lease = createLeaseWithCommitInfo(EncodedUserData.EMPTY.asString());

    // Create V2 lease
    var v2Lease = lease.withNewIndexDefinitionVersion("2", IndexStatus.initialSync());

    // V2's commitInfo should also be empty since there's no highWaterMark to preserve
    assertEquals(EncodedUserData.EMPTY.asString(), v2Lease.commitInfo());
  }

  private Lease createLeaseWithCommitInfo(String commitInfo) {
    return new Lease(
        LEASE_ID,
        Lease.FIRST_LEASE_VERSION,
        COLLECTION_UUID,
        COLLECTION_NAME,
        LEASE_OWNER,
        Instant.now(),
        Lease.FIRST_LEASE_VERSION,
        commitInfo,
        "1",
        Map.of("1",
            new Lease.IndexDefinitionVersionStatus(false, IndexStatus.StatusCode.INITIAL_SYNC)));
  }
}

