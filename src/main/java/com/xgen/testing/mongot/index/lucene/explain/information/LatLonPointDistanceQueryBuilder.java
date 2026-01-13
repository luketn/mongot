package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.Coordinate;
import com.xgen.mongot.index.lucene.explain.information.LatLonPointDistanceQuerySpec;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class LatLonPointDistanceQueryBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<Coordinate> center = Optional.empty();
  private Optional<Double> radius = Optional.empty();

  public static LatLonPointDistanceQueryBuilder builder() {
    return new LatLonPointDistanceQueryBuilder();
  }

  public LatLonPointDistanceQueryBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public LatLonPointDistanceQueryBuilder center(Coordinate center) {
    this.center = Optional.of(center);
    return this;
  }

  public LatLonPointDistanceQueryBuilder radius(double radius) {
    this.radius = Optional.of(radius);
    return this;
  }

  /** Builds the LatLonPointDistanceQuery from the LatLonPointDistanceQueryBuilder. */
  public LatLonPointDistanceQuerySpec build() {
    return new LatLonPointDistanceQuerySpec(this.path, this.center, this.radius);
  }
}
