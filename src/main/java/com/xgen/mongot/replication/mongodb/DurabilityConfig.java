package com.xgen.mongot.replication.mongodb;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Runtime;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.time.Duration;
import java.util.Optional;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurabilityConfig implements DocumentEncodable {

  private static class Fields {
    private static final Field.Required<Integer> NUM_COMMITTING_THREADS =
        Field.builder("numCommittingThreads").intField().mustBePositive().required();
    private static final Field.Required<Integer> COMMIT_INTERVAL =
        Field.builder("commitInterval").intField().required();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DurabilityConfig.class);

  private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofMinutes(1);

  /** The number of threads to run in the Executor in charge of committing indexes. */
  public final int numCommittingThreads;

  /** How long we should wait before committing the index to make updates durable. */
  public final Duration commitInterval;

  private DurabilityConfig(int numCommittingThreads, Duration commitInterval) {
    this.numCommittingThreads = numCommittingThreads;
    this.commitInterval = commitInterval;
  }

  public static DurabilityConfig create(
      Optional<Integer> optionalNumCommittingThreads, Optional<Duration> optionalCommitInterval) {
    return create(Runtime.INSTANCE, optionalNumCommittingThreads, optionalCommitInterval);
  }

  @VisibleForTesting
  static DurabilityConfig create(
      Runtime runtime,
      Optional<Integer> optionalNumCommittingThreads,
      Optional<Duration> optionalCommitInterval) {

    int numCommittingThreads = getNumCommittingThreads(runtime, optionalNumCommittingThreads);
    Check.argIsPositive(numCommittingThreads, "numCommittingThreads");

    Duration commitInterval = optionalCommitInterval.orElse(DEFAULT_COMMIT_INTERVAL);
    checkArg(
        !commitInterval.isNegative() && !commitInterval.isZero(),
        "commit interval must be positive (is %s)",
        commitInterval);

    return new DurabilityConfig(numCommittingThreads, commitInterval);
  }

  private static int getNumCommittingThreads(
      Runtime runtime, Optional<Integer> optionalNumCommittingThreads) {
    return optionalNumCommittingThreads.orElseGet(
        // Limit the default to a maximum of 10 threads.
        () -> {
          int numCommittingThreads =
              Math.max(1, Math.min(10, Math.floorDiv(runtime.getNumCpus(), 2)));
          LOG.info("numCommittingThreads not configured, defaulting to {}.", numCommittingThreads);
          return numCommittingThreads;
        });
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.NUM_COMMITTING_THREADS, this.numCommittingThreads)
        .field(Fields.COMMIT_INTERVAL, Math.toIntExact(this.commitInterval.toMillis()))
        .build();
  }
}
