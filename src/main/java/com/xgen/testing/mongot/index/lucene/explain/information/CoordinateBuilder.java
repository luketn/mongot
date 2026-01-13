package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.Coordinate;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class CoordinateBuilder {
  private Optional<Double> lon = Optional.empty();
  private Optional<Double> lat = Optional.empty();

  public static CoordinateBuilder builder() {
    return new CoordinateBuilder();
  }

  public CoordinateBuilder lon(double lon) {
    this.lon = Optional.of(lon);
    return this;
  }

  public CoordinateBuilder lat(double lat) {
    this.lat = Optional.of(lat);
    return this;
  }

  /** Builds Coordinate from the CoordinateBuilder. */
  public Coordinate build() {
    Check.isPresent(this.lon, "longitude");
    Check.isPresent(this.lat, "latitude");

    return new Coordinate(List.of(this.lon.get(), this.lat.get()));
  }
}
