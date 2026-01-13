package com.xgen.testing.mongot.index.query.sort;

import com.xgen.mongot.index.query.sort.SortOrder;
import java.util.Optional;

public abstract sealed class SortOptionsBuilder<T, B extends SortOptionsBuilder<T, B>>
    permits MetaSortOptionsBuilder, UserFieldSortOptionsBuilder {
  public static MetaSortOptionsBuilder meta() {
    return new MetaSortOptionsBuilder();
  }

  public static UserFieldSortOptionsBuilder user() {
    return new UserFieldSortOptionsBuilder();
  }

  private Optional<SortOrder> sortOrder = Optional.empty();

  public abstract T build();

  abstract B getBuilder();

  public B sortOrder(SortOrder sortOrder) {
    this.sortOrder = Optional.of(sortOrder);
    return getBuilder();
  }

  Optional<SortOrder> getSortOrder() {
    return this.sortOrder;
  }
}
