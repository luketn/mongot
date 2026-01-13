package com.xgen.mongot.metrics.ftdc;

import static com.xgen.mongot.util.Check.checkArg;

import java.util.LinkedHashMap;
import java.util.List;
import org.bson.BsonDocument;

/**
 * MetricChunk
 *
 * @param schema A BSON document specifying the schema of the metric chunk, where the values of each
 *     key are the first sample. When FTDC is decoded, the samples are cast to the types of values
 *     in schema.
 * @param metrics We use a LinkedHashMap to preserve the order of the metrics. In FTDC, the order of
 *     metrics in the schema has to be consistent with the order of compressed metrics array. Note
 *     that metrics also contains the first sample.
 */
public record MetricChunk(
    BsonDocument schema, LinkedHashMap<String, List<Long>> metrics, long epochTime) {
  public MetricChunk {
    checkArg(!metrics.isEmpty(), "empty metrics");
    checkArg(
        metrics.values().stream().findFirst().map(List::size).orElse(0) > 0,
        "samples can't be empty");
  }

  @Override
  public String toString() {
    return String.format("MetricChunk{epochTime=%d, schema=%s}", this.epochTime, this.schema);
  }
}
