package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Comparator;
import java.util.List;
import org.bson.BsonDocument;

/**
 * Records resource usage for a single request with BSON serialization/deserialization.
 *
 * <p>At the moment this includes the following:
 *
 * <ul>
 *   <li>Major page faults (where data is read from backing store).
 *   <li>Minor page faults (where data in page cache is mapped into process page table).
 *   <li>User space CPU time, in ms.
 *   <li>System (kernel) CPU time, in ms.
 *   <li>Max number of reporting threads across all batches
 *   <li>Number of batches
 * </ul>
 */
public record ResourceUsageOutput(
    long majorFaults,
    long minorFaults,
    long userTimeMs,
    long systemTimeMs,
    int maxReportingThreads,
    int numBatches)
    implements DocumentEncodable, Comparable<ResourceUsageOutput> {
  static class Fields {
    static final Field.Required<Long> MAJOR_FAULTS =
        Field.builder("majorFaults").longField().required();
    static final Field.Required<Long> MINOR_FAULTS =
        Field.builder("minorFaults").longField().required();
    static final Field.Required<Long> USER_TIME_MS =
        Field.builder("userTimeMs").longField().required();
    static final Field.Required<Long> SYSTEM_TIME_MS =
        Field.builder("systemTimeMs").longField().required();
    static final Field.Required<Integer> MAX_REPORTING_THREADS =
        Field.builder("maxReportingThreads").intField().required();
    static final Field.Required<Integer> NUM_BATCHES =
        Field.builder("numBatches").intField().required();
  }

  public static ResourceUsageOutput create(List<ResourceUsageCollector> perBatchResourceUsages) {
    ResourceUsageCollector totalResourceUsage =
        perBatchResourceUsages.stream()
            .reduce(ResourceUsageCollector.EMPTY, ResourceUsageCollector::sum);
    int maxReportingThreads =
        perBatchResourceUsages.stream()
            .mapToInt(ResourceUsageCollector::reportingThreads)
            .max()
            .orElse(1);
    int numBatches = perBatchResourceUsages.size();

    return new ResourceUsageOutput(
        totalResourceUsage.majorFaults(),
        totalResourceUsage.minorFaults(),
        totalResourceUsage.userTimeMs(),
        totalResourceUsage.systemTimeMs(),
        maxReportingThreads,
        numBatches);
  }

  public static ResourceUsageOutput fromBson(DocumentParser parser) throws BsonParseException {
    return new ResourceUsageOutput(
        parser.getField(Fields.MAJOR_FAULTS).unwrap(),
        parser.getField(Fields.MINOR_FAULTS).unwrap(),
        parser.getField(Fields.USER_TIME_MS).unwrap(),
        parser.getField(Fields.SYSTEM_TIME_MS).unwrap(),
        parser.getField(Fields.MAX_REPORTING_THREADS).unwrap(),
        parser.getField(Fields.NUM_BATCHES).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MAJOR_FAULTS, this.majorFaults)
        .field(Fields.MINOR_FAULTS, this.minorFaults)
        .field(Fields.USER_TIME_MS, this.userTimeMs)
        .field(Fields.SYSTEM_TIME_MS, this.systemTimeMs)
        .field(Fields.MAX_REPORTING_THREADS, this.maxReportingThreads)
        .field(Fields.NUM_BATCHES, this.numBatches)
        .build();
  }

  @Override
  public int compareTo(ResourceUsageOutput o) {
    return Comparator.comparingLong(ResourceUsageOutput::majorFaults)
        .thenComparingLong(ResourceUsageOutput::minorFaults)
        .thenComparingLong(ResourceUsageOutput::userTimeMs)
        .thenComparingLong(ResourceUsageOutput::systemTimeMs)
        .thenComparingInt(ResourceUsageOutput::maxReportingThreads)
        .thenComparingInt(ResourceUsageOutput::numBatches)
        .compare(this, o);
  }
}
