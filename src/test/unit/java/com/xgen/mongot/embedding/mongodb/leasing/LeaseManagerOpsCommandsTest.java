package com.xgen.mongot.embedding.mongodb.leasing;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.xgen.mongot.embedding.mongodb.leasing.LeaseManagerOpsCommands.OpsGiveUpLeaseCommand;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class LeaseManagerOpsCommandsTest {

  private static final String INSTANCE_1 = "atlas-6hubfx-shard-00-00.ytzdfr.mongodb-dev.net";
  private static final String INSTANCE_2 = "atlas-9kxyz1-shard-00-01.abc123.mongodb-dev.net";
  private static final String LEASE_1 =
      "69a7ab02ac4c64cd5800caaf-66392faf9727adb4c26e76dc37b98b9f-1";
  private static final String LEASE_2 =
      "69a7ab02ac4c64cd5800caaf-77403faf9727adb4c26e76dc37b98b9f-2";

  @Test
  public void getOpsGiveUpLeaseCommand_emptyLeaseNames_returnsCommand() {
    Optional<OpsGiveUpLeaseCommand> result =
        LeaseManagerOpsCommands.getOpsGiveUpLeaseCommand(
            INSTANCE_1, List.of(), "2026-03-06T12:00:00Z");
    assertTrue(result.isPresent());
    assertEquals(INSTANCE_1, result.get().instance());
    assertEquals(List.of(), result.get().leaseNames());
    assertEquals(Instant.parse("2026-03-06T12:00:00Z"), result.get().expiresAt());
  }

  @Test
  public void getOpsGiveUpLeaseCommand_nullExpiresAt_returnsEmpty() {
    assertEquals(
        Optional.empty(),
        LeaseManagerOpsCommands.getOpsGiveUpLeaseCommand(INSTANCE_1, List.of(LEASE_1), null));
  }

  @Test
  public void getOpsGiveUpLeaseCommand_blankExpiresAt_returnsEmpty() {
    assertEquals(
        Optional.empty(),
        LeaseManagerOpsCommands.getOpsGiveUpLeaseCommand(INSTANCE_1, List.of(LEASE_1), ""));
  }

  @Test
  public void getOpsGiveUpLeaseCommand_validIso8601ExpiresAt_returnsCommand() {
    Optional<OpsGiveUpLeaseCommand> result =
        LeaseManagerOpsCommands.getOpsGiveUpLeaseCommand(
            INSTANCE_1, List.of(LEASE_1), "2026-03-06T12:00:00Z");

    assertTrue(result.isPresent());
    assertEquals(INSTANCE_1, result.get().instance());
    assertEquals(List.of(LEASE_1), result.get().leaseNames());
    assertEquals(Instant.parse("2026-03-06T12:00:00Z"), result.get().expiresAt());
  }

  @Test
  public void getOpsGiveUpLeaseCommand_validEpochMsExpiresAt_returnsCommand() {
    Optional<OpsGiveUpLeaseCommand> result =
        LeaseManagerOpsCommands.getOpsGiveUpLeaseCommand(
            INSTANCE_2, List.of(LEASE_1, LEASE_2), "1741262400000");

    assertTrue(result.isPresent());
    assertEquals(INSTANCE_2, result.get().instance());
    assertEquals(List.of(LEASE_1, LEASE_2), result.get().leaseNames());
    assertEquals(Instant.ofEpochMilli(1741262400000L), result.get().expiresAt());
  }

  @Test
  public void getOpsGiveUpLeaseCommand_invalidExpiresAt_returnsEmpty() {
    assertEquals(
        Optional.empty(),
        LeaseManagerOpsCommands.getOpsGiveUpLeaseCommand(
            INSTANCE_1, List.of(LEASE_1), "not-a-date"));
  }

  @Test
  public void create_validConfig_returnsLeaseManagerOpsCommands() {
    LeaseManagerOpsCommands result =
        LeaseManagerOpsCommands.create(INSTANCE_1, List.of(LEASE_1), "2026-03-06T12:00:00Z");

    assertTrue(result.opsGiveUpLease().isPresent());
    assertEquals(INSTANCE_1, result.opsGiveUpLease().get().instance());
  }

  @Test
  public void create_invalidConfig_returnsNone() {
    LeaseManagerOpsCommands result =
        LeaseManagerOpsCommands.create(INSTANCE_1, List.of(LEASE_1), null);

    assertEquals(LeaseManagerOpsCommands.NONE, result);
  }
}
