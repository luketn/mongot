package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.GeoWithinOperator;
import com.xgen.mongot.index.query.shapes.Shape;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class GeoWithinOperatorBuilder
    extends PathOperatorBuilder<GeoWithinOperator, GeoWithinOperatorBuilder> {

  private Optional<Shape> shape = Optional.empty();

  @Override
  protected GeoWithinOperatorBuilder getBuilder() {
    return this;
  }

  public GeoWithinOperatorBuilder shape(Shape shape) {
    this.shape = Optional.of(shape);
    return this;
  }

  /** Builds the operator. */
  @Override
  public GeoWithinOperator build() {
    Check.isPresent(this.shape, "shape");
    return new GeoWithinOperator(getScore(), getPaths(), this.shape.get());
  }
}
