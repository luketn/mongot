package com.xgen.mongot.metrics.micrometer;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;

public record SerializableTimer(
    TimeUnit timeUnit,
    long count,
    double totalTime,
    double max,
    double mean,
    Optional<Percentiles> percentiles)
    implements DocumentEncodable {
  private static class Fields {
    private static final Field.Required<TimeUnit> TIME_UNIT =
        Field.builder("timeUnit").enumField(TimeUnit.class).asCamelCase().required();

    private static final Field.Required<Long> COUNT = Field.builder("count").longField().required();

    // TODO(CLOUDP-110899): rename totalTimeSeconds, maxTimeSeconds, and meanTimeSeconds to
    // totalTime, max, and mean
    private static final Field.Required<Double> TOTAL_TIME =
        Field.builder("totalTimeSeconds").doubleField().required();

    private static final Field.Required<Double> MAX =
        Field.builder("maxTimeSeconds").doubleField().required();

    private static final Field.Required<Double> MEAN =
        Field.builder("meanTimeSeconds").doubleField().required();

    private static final Field.Optional<Percentiles> PERCENTILES =
        Field.builder("percentiles")
            .classField(Percentiles::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  SerializableTimer() {
    this(TimeUnit.NANOSECONDS, 0, 0, 0, 0, Optional.empty());
  }

  public static SerializableTimer create(Optional<Meter> maybeTimer) {
    return maybeTimer.map(SerializableTimer::create).orElseGet(SerializableTimer::new);
  }

  public static SerializableTimer create(Meter maybeTimer) {
    Timer timer = extractTimer(maybeTimer);
    TimeUnit timeUnit = extractTimeUnit(timer);

    HistogramSnapshot timerSnapshot = timer.takeSnapshot();
    Optional<Percentiles> percentiles = extractPercentiles(timerSnapshot, timeUnit);

    return new SerializableTimer(
        timeUnit,
        timerSnapshot.count(),
        timerSnapshot.total(timeUnit),
        timerSnapshot.max(timeUnit),
        timerSnapshot.mean(timeUnit),
        percentiles);
  }

  public static SerializableTimer fromBson(DocumentParser parser) throws BsonParseException {
    return new SerializableTimer(
        parser.getField(SerializableTimer.Fields.TIME_UNIT).unwrap(),
        parser.getField(SerializableTimer.Fields.COUNT).unwrap(),
        parser.getField(SerializableTimer.Fields.TOTAL_TIME).unwrap(),
        parser.getField(SerializableTimer.Fields.MAX).unwrap(),
        parser.getField(SerializableTimer.Fields.MEAN).unwrap(),
        parser.getField(SerializableTimer.Fields.PERCENTILES).unwrap());
  }

  static Timer extractTimer(Meter maybeTimer) {
    checkArg(maybeTimer.getId().getType() == Meter.Type.TIMER, "must be Timer");
    return (Timer) maybeTimer;
  }

  static TimeUnit extractTimeUnit(Timer timer) {
    Optional<String> timeUnitTag = Optional.ofNullable(timer.getId().getTag("timeUnit"));
    return timeUnitTag.map(String::toUpperCase).map(TimeUnit::valueOf).orElse(TimeUnit.SECONDS);
  }

  static Optional<Percentiles> extractPercentiles(
      HistogramSnapshot timerSnapshot, TimeUnit timeUnit) {
    ValueAtPercentile[] percentileValues = timerSnapshot.percentileValues();
    return percentileValues.length > 0
        ? Optional.of(Percentiles.create(percentileValues, Optional.of(timeUnit)))
        : Optional.empty();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(SerializableTimer.Fields.TIME_UNIT, this.timeUnit())
        .field(SerializableTimer.Fields.COUNT, this.count())
        .field(SerializableTimer.Fields.TOTAL_TIME, this.totalTime())
        .field(SerializableTimer.Fields.MAX, this.max())
        .field(SerializableTimer.Fields.MEAN, this.mean())
        .field(SerializableTimer.Fields.PERCENTILES, this.percentiles())
        .build();
  }
}
