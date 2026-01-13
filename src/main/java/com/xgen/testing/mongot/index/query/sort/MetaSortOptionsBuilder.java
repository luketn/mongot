package com.xgen.testing.mongot.index.query.sort;

import com.xgen.mongot.index.query.sort.MetaSortField;
import com.xgen.mongot.index.query.sort.MetaSortOptions;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public final class MetaSortOptionsBuilder
    extends SortOptionsBuilder<MetaSortOptions, MetaSortOptionsBuilder> {
  private Optional<MetaSortField> meta = Optional.empty();

  @Override
  MetaSortOptionsBuilder getBuilder() {
    return this;
  }

  public static MetaSortOptionsBuilder builder() {
    return new MetaSortOptionsBuilder();
  }

  public MetaSortOptionsBuilder meta(MetaSortField meta) {
    this.meta = Optional.of(meta);
    return this;
  }

  @Override
  public MetaSortOptions build() {
    var meta = Check.isPresent(this.meta, "meta");
    var sortOrder = getSortOrder().orElseGet(meta::getDefaultSortOrder);
    return new MetaSortOptions(sortOrder, meta);
  }
}
