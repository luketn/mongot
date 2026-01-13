package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import java.io.IOException;
import org.apache.lucene.index.NumericDocValues;
import org.bson.BsonInt64;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * LeafComparator that reads longs from DocValues values but is able to compare them to other
 * BsonValues within the same sort bracket.
 */
class MixedLongLeafComparator extends MixedNumericLeafComparator {
  private static final int LONG_BRACKET_PRIORITY =
      SortUtil.getBracketPriority(BsonType.INT64, NullEmptySortPosition.LOWEST);

  MixedLongLeafComparator(CompositeComparator parent, NumericDocValues dv) {
    super(parent, dv, LONG_BRACKET_PRIORITY);
  }

  @Override
  public BsonValue getCurrentValue() throws IOException {
    return new BsonInt64(this.dv.longValue());
  }
}
