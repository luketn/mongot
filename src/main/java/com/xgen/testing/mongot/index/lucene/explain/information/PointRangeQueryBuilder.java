package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.PointRangeQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.Representation;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class PointRangeQueryBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<Representation> representation = Optional.empty();
  private Optional<Point> greaterThan = Optional.empty();
  private Optional<Point> lessThan = Optional.empty();

  public static PointRangeQueryBuilder builder() {
    return new PointRangeQueryBuilder();
  }

  public PointRangeQueryBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public PointRangeQueryBuilder representation(Representation representation) {
    this.representation = Optional.of(representation);
    return this;
  }

  public PointRangeQueryBuilder greaterThan(Point greaterThan) {
    this.greaterThan = Optional.of(greaterThan);
    return this;
  }

  public PointRangeQueryBuilder lessThan(Point lessThan) {
    this.lessThan = Optional.of(lessThan);
    return this;
  }

  /** Builds a PointRangeQuery from the PointRangeQueryBuilder. */
  public PointRangeQuerySpec build() {
    Check.isPresent(this.path, "path");

    return new PointRangeQuerySpec(
        this.path.get(), this.representation, this.greaterThan, this.lessThan);
  }
}
