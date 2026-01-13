package com.xgen.mongot.index.lucene.query.pushdown;

import java.util.Comparator;
import org.bson.BsonArray;
import org.bson.BsonValue;

/**
 * The MQL aggregation pipeline implements 3 strategies for comparing arrays.
 *
 * <ol>
 *   <li>In ascending sort, compare min of each array
 *   <li>In descending sort, compare max of each array
 *   <li>In $match inequalities, arrays are compared lexicographically
 * </ol>
 */
public enum ArrayComparator implements Comparator<BsonArray> {
  MIN {

    private BsonValue getRepresentativeValue(BsonArray array) {
      return array.stream()
          .min(MqlComparator.MIN_SELECTING_COMPARATOR)
          .orElse(MqlComparator.UNDEFINED);
    }

    @Override
    public int compare(BsonArray left, BsonArray right) {
      var l = getRepresentativeValue(left);
      var r = getRepresentativeValue(right);
      return MqlComparator.compareValues(l, r, this);
    }
  },
  MAX {

    private BsonValue getRepresentativeValue(BsonArray array) {
      return array.stream()
          .max(MqlComparator.MAX_SELECTING_COMPARATOR)
          .orElse(MqlComparator.UNDEFINED);
    }

    @Override
    public int compare(BsonArray left, BsonArray right) {
      var l = getRepresentativeValue(left);
      var r = getRepresentativeValue(right);
      return MqlComparator.compareValues(l, r, this);
    }
  },
  LEXICOGRAPHIC {

    @Override
    public int compare(BsonArray left, BsonArray right) {
      // Don't use get(i) because it's O(n^2) performance for RawBsonArray
      var l = left.iterator();
      var r = right.iterator();
      while (l.hasNext() && r.hasNext()) {
        int cmp = MqlComparator.compareValues(l.next(), r.next(), this);
        if (cmp != 0) {
          return cmp;
        }
      }

      return Boolean.compare(l.hasNext(), r.hasNext());
    }
  };
}
