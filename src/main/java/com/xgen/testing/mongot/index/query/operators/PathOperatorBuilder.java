package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;

public abstract class PathOperatorBuilder<T, B extends PathOperatorBuilder<T, B>>
    extends OperatorBuilder<T, B> {

  private final List<FieldPath> paths = new ArrayList<>();

  public B path(String path) {
    this.paths.add(FieldPath.parse(path));
    return getBuilder();
  }

  List<FieldPath> getPaths() {
    Check.argNotEmpty(this.paths, "paths");
    return this.paths;
  }
}
