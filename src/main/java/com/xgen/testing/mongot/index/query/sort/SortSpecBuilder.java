package com.xgen.testing.mongot.index.query.sort;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.Sort;
import com.xgen.mongot.index.query.sort.SortBetaV1;
import java.util.ArrayList;
import java.util.List;

public class SortSpecBuilder {
  private final List<MongotSortField> sortFields = new ArrayList<>();

  public static SortSpecBuilder builder() {
    return new SortSpecBuilder();
  }

  public SortSpecBuilder sortField(MongotSortField sortField) {
    this.sortFields.add(sortField);
    return this;
  }

  public SortBetaV1 buildSortBetaV1() {
    checkArg(!this.sortFields.isEmpty(), "Must have 1 or more SortFields");
    return new SortBetaV1(ImmutableList.copyOf(this.sortFields));
  }

  public Sort buildSort() {
    checkArg(!this.sortFields.isEmpty(), "Must have 1 or more SortFields");
    return new Sort(ImmutableList.copyOf(this.sortFields));
  }
}
