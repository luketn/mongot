package com.xgen.testing.mongot.index;

import com.xgen.mongot.index.SearchSortValues;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonValue;

public class SearchSortValuesBuilder {
  List<BsonValue> values = new ArrayList<>();

  public static SearchSortValuesBuilder builder() {
    return new SearchSortValuesBuilder();
  }

  public SearchSortValuesBuilder value(BsonValue value) {
    this.values.add(value);
    return this;
  }

  public SearchSortValues build() {
    Check.argNotEmpty(this.values, "Values cannot be empty");
    return SearchSortValues.create(this.values);
  }
}
