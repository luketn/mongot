package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.NearOperator;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class NearOperatorBuilder extends PathOperatorBuilder<NearOperator, NearOperatorBuilder> {

  private Optional<Point> origin = Optional.empty();
  private Optional<Double> pivot = Optional.empty();

  @Override
  protected NearOperatorBuilder getBuilder() {
    return this;
  }

  public NearOperatorBuilder origin(Point origin) {
    this.origin = Optional.of(origin);
    return this;
  }

  public NearOperatorBuilder pivot(double pivot) {
    this.pivot = Optional.of(pivot);
    return this;
  }

  @Override
  public NearOperator build() {
    Check.isPresent(this.origin, "origin");
    Check.isPresent(this.pivot, "pivot");

    return new NearOperator(getScore(), getPaths(), this.origin.get(), this.pivot.get());
  }
}
