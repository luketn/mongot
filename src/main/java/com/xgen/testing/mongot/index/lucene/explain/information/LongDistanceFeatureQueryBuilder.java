package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.LongDistanceFeatureQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.Representation;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class LongDistanceFeatureQueryBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<Point> origin = Optional.empty();
  private Optional<Point> pivotDistance = Optional.empty();
  private Optional<Representation> representation = Optional.empty();

  public static LongDistanceFeatureQueryBuilder builder() {
    return new LongDistanceFeatureQueryBuilder();
  }

  public LongDistanceFeatureQueryBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public LongDistanceFeatureQueryBuilder origin(Point origin) {
    this.origin = Optional.of(origin);
    return this;
  }

  public LongDistanceFeatureQueryBuilder pivotDistance(Point pivotDistance) {
    this.pivotDistance = Optional.of(pivotDistance);
    return this;
  }

  public LongDistanceFeatureQueryBuilder representation(Representation representation) {
    this.representation = Optional.of(representation);
    return this;
  }

  /** Builds the LongDistanceFeatureQuery from the LongDistanceFeatureQueryBuilder. */
  public LongDistanceFeatureQuerySpec build() {
    return new LongDistanceFeatureQuerySpec(
        this.path, this.origin, this.pivotDistance, this.representation);
  }
}
