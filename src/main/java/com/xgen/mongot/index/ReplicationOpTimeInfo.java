package com.xgen.mongot.index;

import static java.lang.Math.max;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.bson.BsonTimestamp;
import org.jetbrains.annotations.TestOnly;

/**
 * ReplicationOpTimeInfo is a thread-safe class for storing and retrieving observations of the
 * current replication optime.
 *
 * <p>ReplicationLag is updated as we process change stream batches. We calculate replicationLag by
 * subtracting the {@link com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils opTime of the
 * resumeToken} from the {@code operationTime} of the {@link
 * com.xgen.mongot.util.mongodb.ChangeStreamResponse ChangeStreamResponse}.
 *
 * <p>The {@code opTime} of the resumeToken is the timestamp of the batch currently being processed
 * by mongot whereas the {@code operationTime} of the ChangeStreamResponse represents the maximum
 * possible opTime mongot can replicate up until.
 */
public class ReplicationOpTimeInfo {
  private final AtomicReference<Optional<Snapshot>> opTimeSnapshot =
      new AtomicReference<>(Optional.empty());

  private final Object unsetLock = new Object();

  public ReplicationOpTimeInfo() {}

  @VisibleForTesting
  ReplicationOpTimeInfo(long replicationOpTime, long maxPossibleReplicationOpTime) {
    this.opTimeSnapshot.set(
        Optional.of(Snapshot.create(replicationOpTime, maxPossibleReplicationOpTime)));
  }

  @VisibleForTesting
  public ReplicationOpTimeInfo(
      long replicationOpTime, long maxPossibleReplicationOpTime, long replicationLagMs) {
    this.opTimeSnapshot.set(
        Optional.of(
            new Snapshot(replicationOpTime, maxPossibleReplicationOpTime, replicationLagMs)));
  }

  /**
   * Updates this optime info container when only maxPossibleReplicationOpTime is available.
   *
   * <p>{@code STEADY} and {@code RECOVERING_TRANSIENT} statuses do not store replication optime so
   * the most recently updated replication optime is used in that case.
   */
  // This function is only called from the periodic replication optime updater
  public void update(long maxPossibleReplicationOpTime) {
    this.opTimeSnapshot.updateAndGet(
        current ->
            current.map(
                info ->
                    Snapshot.create(
                        info.replicationOpTime,
                        max(info.maxPossibleReplicationOpTime, maxPossibleReplicationOpTime))));
  }

  public void update(long replicationOpTime, long maxPossibleReplicationOpTime) {
    this.opTimeSnapshot.updateAndGet(
        current ->
            current
                .map(
                    info ->
                        // Only update if optime is increasing, or if optime info has not yet been
                        // set
                        Snapshot.create(
                            replicationOpTime,
                            max(info.maxPossibleReplicationOpTime, maxPossibleReplicationOpTime)))
                .or(
                    () ->
                        Optional.of(
                            Snapshot.create(replicationOpTime, maxPossibleReplicationOpTime))));
  }

  /**
   * Returns the object whose monitor is acquired on every unset operation. To inhibit calls to
   * {@link #unset(BooleanSupplier)} or ensure that unset operations do not proceed until other
   * external state is updated, synchronize on the returned object.
   */
  public Object getUnsetLock() {
    return this.unsetLock;
  }

  /**
   * Acquires the unset lock, evaluates the provided condition, and unsets the optime info if the
   * condition is true.
   *
   * <p>In practice, the provided condition should ensure that the associated index is not in a
   * queryable state, to prevent spuriously unsetting the optime info.
   *
   * @param shouldUnset condition to be evaluated while the unset lock is held to determine if the
   *     unset operation should proceed
   * @return {@code true} if the unset operation succeeded, {@code false} otherwise.
   */
  public boolean unset(BooleanSupplier shouldUnset) {
    synchronized (this.unsetLock) {
      if (shouldUnset.getAsBoolean()) {
        this.opTimeSnapshot.set(Optional.empty());
        return true;
      }
      return false;
    }
  }

  public Optional<Snapshot> snapshot() {
    return this.opTimeSnapshot.get();
  }

  @TestOnly
  public Snapshot snapshotOrThrow() {
    return this.opTimeSnapshot.get().orElseThrow();
  }

  public record Snapshot(
      long replicationOpTime, long maxPossibleReplicationOpTime, long replicationLagMs) {

    public static Snapshot create(long replicationOpTime, long maxPossibleReplicationOpTime) {
      var maxOptime = max(replicationOpTime, maxPossibleReplicationOpTime);
      return new Snapshot(
          replicationOpTime, maxOptime, calculateReplicationLagMs(replicationOpTime, maxOptime));
    }

    private static long calculateReplicationLagMs(
        long replicationOpTime, long maxPossibleReplicationOpTime) {
      return Duration.between(
              Instant.ofEpochSecond(new BsonTimestamp(replicationOpTime).getTime()),
              Instant.ofEpochSecond(new BsonTimestamp(maxPossibleReplicationOpTime).getTime()))
          .toMillis();
    }
  }
}
