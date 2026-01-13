package com.xgen.mongot.index.query.sort;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;

/**
 * This enum represents a list of supported sorts on meta fields.
 *
 * <p>Meta fields are computed fields or values indexed implicitly by us or Lucene that users cannot
 * reference directly. Currently, we only support 'searchScore' but potentially this could be
 * expanded for {@code doc_id} or {@link com.xgen.mongot.index.lucene.field.FieldName.MetaField#ID}
 *
 * <p>These values are declared as an enum so that invalid keys raise {@link
 * com.xgen.mongot.util.bson.parser.BsonParseException}. Note that the parser expects the lower
 * camelCase format of the enum values to match the user-supplied $meta value.
 */
public enum MetaSortField {

  // Note: if you add a new value to this enum, you may need to update the encoding for
  // SearchResult.$searchSortValues
  SEARCH_SCORE {
    @Override
    SortField getSortField(SortOptions options) {
      // Note: Lucene uses reverse=True for ascending sort on score, which is the opposite of
      // how Lucene treats every other sort field.
      boolean reverse = options.order() == SortOrder.ASC;
      return new SortField(null, Type.SCORE, reverse);
    }

    @Override
    public SortOrder getDefaultSortOrder() {
      return SortOrder.DESC;
    }
  };

  /** Constructs a {@link SortField} from the given {@link SortOptions}. */
  abstract SortField getSortField(SortOptions options);

  public abstract SortOrder getDefaultSortOrder();
}
