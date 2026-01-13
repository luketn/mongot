package com.xgen.testing.mongot.index.query.sort;

import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.Check;

public final class UserFieldSortOptionsBuilder
    extends SortOptionsBuilder<UserFieldSortOptions, UserFieldSortOptionsBuilder> {
  private NullEmptySortPosition nullEmptySortPosition = NullEmptySortPosition.LOWEST;

  @Override
  UserFieldSortOptionsBuilder getBuilder() {
    return this;
  }

  public static UserFieldSortOptionsBuilder builder() {
    return new UserFieldSortOptionsBuilder();
  }

  public UserFieldSortOptionsBuilder nullEmptySortPosition(
      NullEmptySortPosition nullEmptySortPosition) {
    this.nullEmptySortPosition = nullEmptySortPosition;
    return this;
  }

  @Override
  public UserFieldSortOptions build() {
    var sortOrder = Check.isPresent(getSortOrder(), "sortOrder");
    return new UserFieldSortOptions(sortOrder, this.nullEmptySortPosition);
  }
}
