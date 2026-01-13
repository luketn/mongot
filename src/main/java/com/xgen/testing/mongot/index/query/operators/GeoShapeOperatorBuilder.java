package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.mongot.index.query.shapes.GeometryShape;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class GeoShapeOperatorBuilder
    extends PathOperatorBuilder<GeoShapeOperator, GeoShapeOperatorBuilder> {

  private Optional<GeometryShape> geometry = Optional.empty();
  private Optional<GeoShapeOperator.Relation> relation = Optional.empty();

  @Override
  protected GeoShapeOperatorBuilder getBuilder() {
    return this;
  }

  public GeoShapeOperatorBuilder geometry(GeometryShape geometry) {
    this.geometry = Optional.of(geometry);
    return this;
  }

  public GeoShapeOperatorBuilder relation(GeoShapeOperator.Relation relation) {
    this.relation = Optional.of(relation);
    return this;
  }

  /** Builds the operator. */
  @Override
  public GeoShapeOperator build() {
    Check.isPresent(this.geometry, "geometry");
    Check.isPresent(this.relation, "relation");
    return new GeoShapeOperator(getScore(), getPaths(), this.geometry.get(), this.relation.get());
  }
}
