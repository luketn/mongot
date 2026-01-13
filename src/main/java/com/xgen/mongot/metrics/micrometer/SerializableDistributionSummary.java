package com.xgen.mongot.metrics.micrometer;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.util.Optional;
import org.bson.BsonDocument;

public record SerializableDistributionSummary(
    long count, double total, double max, double mean, Optional<Percentiles> percentiles)
    implements DocumentEncodable {
  private static class Fields {
    private static final Field.Required<Long> COUNT = Field.builder("count").longField().required();

    private static final Field.Required<Double> TOTAL_EVENT_AMOUNT =
        Field.builder("total").doubleField().required();

    private static final Field.Required<Double> MAX = Field.builder("max").doubleField().required();

    private static final Field.Required<Double> MEAN =
        Field.builder("mean").doubleField().required();

    private static final Field.Optional<Percentiles> PERCENTILES =
        Field.builder("percentiles")
            .classField(Percentiles::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  SerializableDistributionSummary() {
    this(0, 0, 0, 0, Optional.empty());
  }

  public static SerializableDistributionSummary create(Optional<Meter> maybeDistributionSummary) {
    return maybeDistributionSummary
        .map(SerializableDistributionSummary::create)
        .orElseGet(SerializableDistributionSummary::new);
  }

  public static SerializableDistributionSummary create(Meter maybeDistributionSummary) {
    DistributionSummary distributionSummary = extractDistributionSummary(maybeDistributionSummary);
    HistogramSnapshot histogramSnapshot = distributionSummary.takeSnapshot();
    Optional<Percentiles> percentiles = extractPercentiles(histogramSnapshot);

    return new SerializableDistributionSummary(
        distributionSummary.count(),
        distributionSummary.totalAmount(),
        distributionSummary.max(),
        distributionSummary.mean(),
        percentiles);
  }

  public static SerializableDistributionSummary fromBson(DocumentParser parser)
      throws BsonParseException {
    return new SerializableDistributionSummary(
        parser.getField(Fields.COUNT).unwrap(),
        parser.getField(Fields.TOTAL_EVENT_AMOUNT).unwrap(),
        parser.getField(Fields.MAX).unwrap(),
        parser.getField(Fields.MEAN).unwrap(),
        parser.getField(Fields.PERCENTILES).unwrap());
  }

  static DistributionSummary extractDistributionSummary(Meter maybeDistributionSummary) {
    checkArg(
        maybeDistributionSummary.getId().getType() == Meter.Type.DISTRIBUTION_SUMMARY,
        "must be DistributionSummary");
    return (DistributionSummary) maybeDistributionSummary;
  }

  static Optional<Percentiles> extractPercentiles(HistogramSnapshot histogramSnapshot) {
    ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
    return percentileValues.length > 0
        ? Optional.of(Percentiles.create(percentileValues, Optional.empty()))
        : Optional.empty();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.COUNT, this.count())
        .field(Fields.TOTAL_EVENT_AMOUNT, this.total())
        .field(Fields.MAX, this.max())
        .field(Fields.MEAN, this.mean())
        .field(Fields.PERCENTILES, this.percentiles())
        .build();
  }
}
