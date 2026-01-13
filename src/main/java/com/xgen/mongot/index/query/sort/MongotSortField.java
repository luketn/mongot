package com.xgen.mongot.index.query.sort;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonValue;

/**
 * Specifies one component of a {@link SortSpec}: a field and its sort options. Analogous to the
 * Lucene class {@link org.apache.lucene.search.SortField}.
 */
public record MongotSortField(FieldPath field, SortOptions options) {
  public static MongotSortField fromBson(String key, BsonValue value, BsonParseContext context)
      throws BsonParseException {
    var order = SortOptions.fromBson(context, value);
    return new MongotSortField(FieldPath.parse(key), order);
  }

  public MongotSortField invert() {
    return new MongotSortField(this.field, this.options.invert());
  }
}
