package com.xgen.mongot.index.lucene.query.sort.comparator;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.search.comparators.NumericComparator;

/**
 * MqlLongComparator is able to sort Long values in a way that's compliant with the MQL sort order.
 * The position of missing values depends on the {@link NullEmptySortPosition} passed in.
 *
 * <p>This is mostly a copy of {@link LongComparator}, but with missing values stored explicitly as
 * null.
 */
public class MqlLongComparator extends NumericComparator<Long> {
  private final Long[] values;
  protected @Nullable Long topValue;
  protected @Nullable Long bottom;
  private final SortField.Type sortType;
  private final SortedNumericSelector.Type selector;
  private final NullEmptySortPosition nullEmptySortPosition;

  /**
   * Records the initial pruning configuration set by Lucene. The pruning configuration may be
   * modified depending on the bottom value and the type of sort requested, as described below in
   * {@link MqlLongComparator.LongLeafComparator#encodeBottom(byte[])}}.
   */
  private final Pruning originalPruningConfig;

  public MqlLongComparator(
      int numHits,
      String field,
      boolean reverse,
      Pruning pruning,
      SortField.Type sortType,
      SortedNumericSelector.Type selector,
      NullEmptySortPosition nullEmptySortPosition) {
    super(field, null, reverse, pruning, Long.BYTES);
    this.values = new Long[numHits];
    this.sortType = sortType;
    this.selector = selector;

    // record initial pruning configuration set by Lucene
    this.originalPruningConfig = pruning;
    this.nullEmptySortPosition = nullEmptySortPosition;
  }

  @Override
  public int compare(int slot1, int slot2) {
    return compareValues(this.values[slot1], this.values[slot2]);
  }

  @Override
  public int compareValues(Long first, Long second) {
    return mqlLongCompare(first, second, this.nullEmptySortPosition);
  }

  @Override
  public void setTopValue(Long value) {
    super.setTopValue(value);
    this.topValue = value;
  }

  @Override
  public Long value(int slot) {
    return this.values[slot];
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new LongLeafComparator(context);
  }

  public class LongLeafComparator extends NumericLeafComparator {

    public LongLeafComparator(LeafReaderContext context) throws IOException {
      super(context);
    }

    @Nullable
    public Long getValueForDoc(int doc) throws IOException {
      if (LongLeafComparator.this.docValues.advanceExact(doc)) {
        return LongLeafComparator.this.docValues.longValue();
      } else {
        // Explicitly return null if the value is missing
        return null;
      }
    }

    @Override
    public void setBottom(int slot) throws IOException {
      MqlLongComparator.this.bottom = MqlLongComparator.this.values[slot];
      super.setBottom(slot);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return mqlLongCompare(
          MqlLongComparator.this.bottom,
          getValueForDoc(doc),
          MqlLongComparator.this.nullEmptySortPosition);
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return mqlLongCompare(
          MqlLongComparator.this.topValue,
          getValueForDoc(doc),
          MqlLongComparator.this.nullEmptySortPosition);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      MqlLongComparator.this.values[slot] = getValueForDoc(doc);
      super.copy(slot, doc);
    }

    /**
     * Determines if a missing value is currently competitive in this sort by comparing it to the
     * existing bottom value. This method is called as part of a preliminary check when trying to
     * prune documents in {@link NumericLeafComparator#updateCompetitiveIterator()}. It ensures that
     * we aren't incorrectly discarding documents containing missing values if they're still
     * competitive.
     *
     * <p>Note that if bottom==null, bottom is encoded as Long.MIN_VALUE or Long.MAX_VALUE depending
     * on the {@link NullEmptySortPosition} when pruning. In that case, mongot should be careful not
     * to discard documents containing actual values of Long.MIN_VALUE / Long.MAX_VALUE from the
     * result set.
     *
     * <p>For a simpler example, if the bottom == 5 and an ascending sort with nulls: lowest is
     * being run, then missing values are still competitive since they're weaker than 5 in an asc
     * sort so the caller method {@link NumericLeafComparator#isMissingValueCompetitive()} should
     * return true.
     */
    @Override
    protected int compareMissingValueWithBottomValue() {
      // Reset the pruning config to its original state before evaluating if the missing value is
      // competitive with the bottom value, since previous calls to encodeBottom() may have modified
      // the Pruning enum if pruning was performed earlier.
      MqlLongComparator.super.pruning = MqlLongComparator.this.originalPruningConfig;

      return mqlLongCompare(
          MqlLongComparator.this.missingValue,
          MqlLongComparator.this.bottom,
          MqlLongComparator.this.nullEmptySortPosition);
    }

    @Override
    protected int compareMissingValueWithTopValue() {
      return mqlLongCompare(
          MqlLongComparator.this.missingValue,
          MqlLongComparator.this.topValue,
          MqlLongComparator.this.nullEmptySortPosition);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      // If bottom is null and nulls should be treated as lowest, encode bottom as Long.MIN_VALUE.
      // If nulls should be treated as highest, encode bottom as Long.MAX_VALUE. Note that docs
      // with nulls are treated as equivalent to docs that contain Long.MIN_VALUE / Long.MAX_VALUE
      // depending on the encoding when generating candidates for the competitiveIterator. These
      // docs will be correctly ordered by the MqlLongComparator.
      long encodedBottom =
          MqlLongComparator.this.bottom != null
              ? MqlLongComparator.this.bottom
              : MqlLongComparator.this.nullEmptySortPosition == NullEmptySortPosition.HIGHEST
                  ? Long.MAX_VALUE
                  : Long.MIN_VALUE;
      LongPoint.encodeDimension(encodedBottom, packedValue, 0);

      if (MqlLongComparator.this.reverse
          && MqlLongComparator.this.nullEmptySortPosition != NullEmptySortPosition.HIGHEST
          && (MqlLongComparator.this.bottom == null)) {
        // For a descending sort with nulls lowest, we set the pruning enum to Pruning.GREATER_THAN
        // to ensure that documents containing actual Long.MIN_VALUE-s are not discarded when
        // bottom == null.
        MqlLongComparator.super.pruning = Pruning.GREATER_THAN;
        return;
      } else if (!MqlLongComparator.this.reverse
          && MqlLongComparator.this.nullEmptySortPosition == NullEmptySortPosition.HIGHEST
          && (MqlLongComparator.this.bottom == null)) {
        // For an ascending sort with nulls highest, we set the pruning enum to Pruning.GREATER_THAN
        // to ensure that documents containing actual Long.MAX_VALUE-s are not discarded when
        // bottom == null.
        MqlLongComparator.super.pruning = Pruning.GREATER_THAN;
        return;
      }
      // In all other sort scenarios, mongot can prune according to its original pruning
      // configuration defined at this class's instantiation
      MqlLongComparator.super.pruning = MqlLongComparator.this.originalPruningConfig;
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      // Same logic from encodeBottom applies in encodeTop. Since Lucene's NumericComparator always
      // calls encodeBottom() before encodeTop() when about to prune, the pruning configuration has
      // already been set.
      long encodedTop =
          MqlLongComparator.this.topValue != null
              ? MqlLongComparator.this.topValue
              : MqlLongComparator.this.nullEmptySortPosition == NullEmptySortPosition.HIGHEST
                  ? Long.MAX_VALUE
                  : Long.MIN_VALUE;
      LongPoint.encodeDimension(encodedTop, packedValue, 0);
    }

    @Override
    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
        throws IOException {
      return SortedNumericSelector.wrap(
          DocValues.getSortedNumeric(context.reader(), field),
          MqlLongComparator.this.selector,
          MqlLongComparator.this.sortType);
    }
  }

  @VisibleForTesting
  static int mqlLongCompare(
      @Nullable Long first, @Nullable Long second, NullEmptySortPosition nullEmptySortPosition) {
    if (first == null && second == null) {
      return 0;
    }

    if (first == null) {
      return nullEmptySortPosition == NullEmptySortPosition.HIGHEST ? 1 : -1;
    }

    if (second == null) {
      return nullEmptySortPosition == NullEmptySortPosition.HIGHEST ? -1 : 1;
    }

    return Long.compare(first, second);
  }
}
