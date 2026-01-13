package com.xgen.mongot.metrics.ftdc;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.Lists;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Collects samples of metrics. When the schema of incoming metrics is changed (metrics added or
 * removed) this collector will return a {@link MetricChunk} with all metrics sampled so far.
 */
public class FtdcCollector {
  private Optional<SchemaCollector> schemaCollector;

  FtdcCollector() {
    this.schemaCollector = Optional.empty();
  }

  /**
   * Collect metrics from a flat bson document representing one sample taken at epochTime.
   *
   * <p>returns a {@link MetricChunk} if it is time to flush it due to a schema change.
   */
  public Optional<MetricChunk> collect(BsonDocument sample, long epochTime) {
    LinkedHashMap<String, Long> extracted = extractMetrics(sample);
    checkArg(!extracted.isEmpty(), "metric sample has no numeric values");

    // first sample to be seen
    if (this.schemaCollector.isEmpty()) {
      this.schemaCollector = Optional.of(new SchemaCollector(sample, extracted, epochTime));
      return Optional.empty();
    }

    SchemaCollector collector = this.schemaCollector.get();
    boolean schemaChanged = collector.collect(extracted);

    if (!schemaChanged) {
      return Optional.empty();
    }

    // schema has changed, set up a new collector
    MetricChunk previousChunk = collector.getChunk();
    this.schemaCollector = Optional.of(new SchemaCollector(sample, extracted, epochTime));
    return Optional.of(previousChunk);
  }

  /** Wipes out any current samples. */
  void clear() {
    this.schemaCollector = Optional.empty();
  }

  /** returns a chunk if one is currently buffered. */
  Optional<MetricChunk> getCurrentChunk() {
    return this.schemaCollector.map(SchemaCollector::getChunk);
  }

  int getNumSamples() {
    return this.schemaCollector.map(SchemaCollector::size).orElse(0);
  }

  static LinkedHashMap<String, Long> extractMetrics(BsonDocument sample) {
    return sample.entrySet().stream()
        .flatMap(m -> getValueFromBson(FieldPath.newRoot(m.getKey()), m.getValue()).stream())
        .collect(
            Collectors.toMap(
                e -> e.getKey().toString(), Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
  }

  private static List<Pair<FieldPath, Long>> getValueFromBson(FieldPath path, BsonValue value) {
    return switch (value.getBsonType()) {
      case INT32, INT64, DOUBLE ->
          // doubles are truncated to the nearest long.
          List.of(Pair.of(path, value.asNumber().longValue()));
      case BOOLEAN -> List.of(Pair.of(path, value.asBoolean().getValue() ? 1L : 0L));
      case DATE_TIME -> List.of(Pair.of(path, value.asDateTime().getValue()));
      case TIMESTAMP ->
          // deltas are supplied separately for time and inc, so adding both of them
          List.of(
              Pair.of(path, (long) value.asTimestamp().getTime()),
              Pair.of(path.newChild("inc"), (long) value.asTimestamp().getInc()));
      case ARRAY -> {
        var counter = new AtomicInteger();
        yield value.asArray().stream()
            .flatMap(
                val ->
                    getValueFromBson(path.newChild(String.valueOf(counter.incrementAndGet())), val)
                        .stream())
            .collect(Collectors.toList());
      }
      case DOCUMENT ->
          // flatten all nested metrics
          value.asDocument().entrySet().stream()
              .flatMap(e -> getValueFromBson(path.newChild(e.getKey()), e.getValue()).stream())
              .collect(Collectors.toList());
      default -> List.of();
    };
  }

  /** Collects metrics as long as the schema remains the same. */
  private static class SchemaCollector {
    private final BsonDocument schema;
    // The order of samples here has to be consistent with the order in schema.
    private final LinkedHashMap<String, List<Long>> samples;
    private final long epochTime;

    SchemaCollector(BsonDocument sample, LinkedHashMap<String, Long> extracted, long epochTime) {
      // the first sample serves as the schema document.
      this.schema = sample;
      this.epochTime = epochTime;

      this.samples = new LinkedHashMap<>(extracted.size());
      extracted.forEach((key, value) -> this.samples.put(key, Lists.newArrayList(value)));
    }

    /** Returns true if the schema has changed. */
    boolean collect(Map<String, Long> newSample) {
      if (schemaChanged(newSample)) {
        return true;

      } else {
        appendSample(newSample);
        return false;
      }
    }

    private boolean schemaChanged(Map<String, Long> newSample) {
      Set<String> reference = this.samples.keySet();
      Set<String> newSamples = newSample.keySet();
      return !reference.equals(newSamples);
    }

    private void appendSample(Map<String, Long> newSample) {
      for (var entry : newSample.entrySet()) {
        List<Long> values = this.samples.get(entry.getKey());
        Check.argNotNull(values, "values");
        values.add(entry.getValue());
      }
    }

    private MetricChunk getChunk() {
      return new MetricChunk(this.schema, this.samples, this.epochTime);
    }

    private int size() {
      return this.samples.values().stream()
          .findFirst()
          .orElseThrow(
              () ->
                  new AssertionError("must have at least one key corresponding to one metric name"))
          .size();
    }
  }
}
