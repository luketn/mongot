package com.xgen.mongot.index.query.sort;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonValue;

public record SortBetaV1(ImmutableList<MongotSortField> fields) implements SortSpec {
  @Override
  public SortBetaV1 invert() {
    var invertedSortFields =
        this.fields.stream().map(MongotSortField::invert).collect(toImmutableList());
    return new SortBetaV1(invertedSortFields);
  }

  @Override
  public ImmutableList<MongotSortField> getSortFields() {
    return this.fields;
  }

  public static SortSpec fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    var fields = SortSpec.getSortFieldsFromBson(context, value);
    if (fields.stream().anyMatch(f -> f.options() instanceof MetaSortOptions)) {
      return context.handleSemanticError("Sort on meta fields is not supported in sortBetaV1");
    }
    return new SortBetaV1(fields);
  }
}
