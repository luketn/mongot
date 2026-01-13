package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.PointInSetQuerySpec;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

public class PointInSetQueryBuilder {

  private Optional<FieldPath> path = Optional.empty();
  private Optional<List<Long>> points = Optional.empty();

  public static PointInSetQueryBuilder builder() {
    return new PointInSetQueryBuilder();
  }

  public PointInSetQueryBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public PointInSetQueryBuilder points(List<Long> points) {
    this.points = Optional.of(points);
    return this;
  }

  public PointInSetQuerySpec build() {
    Check.isPresent(this.path, "path");
    Check.isPresent(this.points, "points");

    return new PointInSetQuerySpec(this.path.get(), this.points.get());
  }
}
