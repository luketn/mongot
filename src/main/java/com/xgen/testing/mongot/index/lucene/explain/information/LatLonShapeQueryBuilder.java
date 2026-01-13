package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.Coordinate;
import com.xgen.mongot.index.lucene.explain.information.LatLonShapeQuerySpec;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

public class LatLonShapeQueryBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<List<List<Coordinate>>> coords = Optional.empty();

  public static LatLonShapeQueryBuilder builder() {
    return new LatLonShapeQueryBuilder();
  }

  public LatLonShapeQueryBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public LatLonShapeQueryBuilder coords(List<List<Coordinate>> coords) {
    this.coords = Optional.of(coords);
    return this;
  }

  /** Builds LatLonShapeQuery from a LatLonShapeQueryBuilder. */
  public LatLonShapeQuerySpec build() {
    return new LatLonShapeQuerySpec(this.path, this.coords);
  }
}
