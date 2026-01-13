package com.xgen.testing.mongot.index.query.sort;

import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortOptions;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class SortFieldBuilder {
  private Optional<FieldPath> path;
  private Optional<SortOptions> sortOption;

  public static SortFieldBuilder builder() {
    return new SortFieldBuilder();
  }

  public SortFieldBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public SortFieldBuilder sortOption(SortOptions sortOptions) {
    this.sortOption = Optional.of(sortOptions);
    return this;
  }

  public MongotSortField build() {
    var path = Check.isPresent(this.path, "path");
    var sortOption = Check.isPresent(this.sortOption, "sortOption");
    return new MongotSortField(path, sortOption);
  }
}
