package com.xgen.mongot.index.query.sort;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonValue;

public record Sort(ImmutableList<MongotSortField> fields) implements SortSpec {

  /** Inverse of Lucene's default sort order. */
  public static final SortSpec REVERSE_RELEVANCE =
      new Sort(
          ImmutableList.of(
              new MongotSortField(
                  FieldPath.newRoot("score"),
                  new MetaSortOptions(SortOrder.ASC, MetaSortField.SEARCH_SCORE))));

  @Override
  public Sort invert() {
    var invertedSortFields =
        this.fields.stream().map(MongotSortField::invert).collect(toImmutableList());
    return new Sort(invertedSortFields);
  }

  @Override
  public ImmutableList<MongotSortField> getSortFields() {
    return this.fields;
  }

  public static SortSpec fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return fromBsonAsSort(context, value);
  }

  public static Sort fromBsonAsSort(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    var fields = SortSpec.getSortFieldsFromBson(context, value);
    return new Sort(fields);
  }
}
