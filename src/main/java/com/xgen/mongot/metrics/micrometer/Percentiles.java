package com.xgen.mongot.metrics.micrometer;

import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.bson.BsonDocument;

public record Percentiles(
    Optional<Double> min,
    Optional<Double> percentile50,
    Optional<Double> percentile75,
    Optional<Double> percentile90,
    Optional<Double> percentile99)
    implements DocumentEncodable {
  private static class Fields {
    private static final Field.Optional<Double> MIN =
        Field.builder("min").doubleField().optional().noDefault();
    private static final Field.Optional<Double> PERCENTILE_50TH =
        Field.builder("50th").doubleField().optional().noDefault();

    private static final Field.Optional<Double> PERCENTILE_75TH =
        Field.builder("75th").doubleField().optional().noDefault();

    private static final Field.Optional<Double> PERCENTILE_90TH =
        Field.builder("90th").doubleField().optional().noDefault();

    private static final Field.Optional<Double> PERCENTILE_99TH =
        Field.builder("99th").doubleField().optional().noDefault();
  }

  public static Percentiles create(
      ValueAtPercentile[] percentileValues, Optional<TimeUnit> timeUnit) {
    Map<Double, Double> percentileToValue =
        Stream.of(percentileValues)
            .collect(
                CollectionUtils.toMapUnsafe(
                    ValueAtPercentile::percentile,
                    valueAtPercentile ->
                        timeUnit
                            .map(valueAtPercentile::value)
                            .orElseGet(valueAtPercentile::value)));

    return new Percentiles(
        Optional.ofNullable(percentileToValue.get(0.0)),
        Optional.ofNullable(percentileToValue.get(0.5)),
        Optional.ofNullable(percentileToValue.get(0.75)),
        Optional.ofNullable(percentileToValue.get(0.9)),
        Optional.ofNullable(percentileToValue.get(0.99)));
  }

  public static Percentiles fromBson(DocumentParser parser) throws BsonParseException {
    return new Percentiles(
        parser.getField(Fields.MIN).unwrap(),
        parser.getField(Fields.PERCENTILE_50TH).unwrap(),
        parser.getField(Fields.PERCENTILE_75TH).unwrap(),
        parser.getField(Fields.PERCENTILE_90TH).unwrap(),
        parser.getField(Fields.PERCENTILE_99TH).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MIN, this.min)
        .field(Fields.PERCENTILE_50TH, this.percentile50)
        .field(Fields.PERCENTILE_75TH, this.percentile75)
        .field(Fields.PERCENTILE_90TH, this.percentile90)
        .field(Fields.PERCENTILE_99TH, this.percentile99)
        .build();
  }
}
