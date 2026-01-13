package com.xgen.mongot.index.lucene.query.sort.mixed;

import com.google.common.collect.Comparators;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.sort.SortOrder;
import com.xgen.mongot.index.query.sort.SortSelector;
import java.io.IOException;
import java.util.Comparator;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

/**
 * The goal of this class is to translate calls from {@link LeafFieldComparator} to the minimal
 * number of calls to its constituent {@link MixedLeafFieldComparator}. For example, if we read an
 * int from a document during an ascending sort, it's never necessary to check if it has a string.
 */
class CompositeLeafComparator implements LeafFieldComparator {

  private static final Comparator<BsonValue> SAME_BRACKET_COMPARATOR =
      SortUtil::compareWithinBracketUnsafe;

  final CompositeComparator parent;
  private final MixedLeafFieldComparator[] children;
  private final int[] bracketEnds;

  /** The sort bracket used for null and missing. */
  private final int nullBracket;

  /**
   * This value is 1 if {@link SortOrder} is ASC, and -1 otherwise. Note this is not necessarily the
   * same as `selectMultiplier` in {@link CompositeComparator} which corresponds to the MIN/MAX
   * selector.
   */
  private final int orderMultiplier;

  /**
   * This is the most competitive child according to {@code
   * orderMultiplier*child.getBracketPriority()}. It's either the first or last child in `children`
   * depending on whether selectMultiplier and orderMultiplier are the same.
   */
  private final MixedLeafFieldComparator mostCompetitiveChild;

  /** Must be set before compareBottom() */
  private int bottomBracket = CompositeComparator.BRACKET_NOT_SET;

  /** Initially set to ALL_DOCS, and constrained as top/bottom is updated. */
  private DocIdSetIterator competitiveIterator;

  CompositeLeafComparator(
      CompositeComparator parent, MixedLeafFieldComparator[] children, LeafReader reader) {
    this.parent = parent;
    this.children = children;
    this.nullBracket = this.parent.options.nullEmptySortPosition().nullMissingPriority;

    // Pre-compute ranges of indices that share same priority using DP
    this.bracketEnds = new int[this.children.length];
    this.bracketEnds[this.bracketEnds.length - 1] = this.bracketEnds.length;
    for (int i = this.children.length - 2; i >= 0; --i) {
      this.bracketEnds[i] =
          this.children[i].getBracketPriority() == this.children[i + 1].getBracketPriority()
              ? this.bracketEnds[i + 1]
              : i + 1;
    }
    this.orderMultiplier = parent.getOrder().isReverse() ? -1 : 1;
    this.mostCompetitiveChild =
        this.orderMultiplier == parent.selectMultiplier
            ? children[0]
            : children[children.length - 1];
    this.competitiveIterator = DocIdSetIterator.all(reader.maxDoc());
  }

  @Override
  public void setBottom(int slot) throws IOException {
    // Called by Lucene's TopFieldCollector
    this.parent.cacheBottomSlot(slot);
    informBottom(this.parent.value(slot));
  }

  /**
   * Called indirectly by Lucene upon a new bottom value. Also, called directly by parent after
   * instantiation.
   */
  void informBottom(BsonValue bottom) throws IOException {
    // Maybe called by parent after init
    int oldBottomBracket = this.bottomBracket;
    this.bottomBracket =
        SortUtil.getBracketPriority(
            bottom.getBsonType(), this.parent.options.nullEmptySortPosition());

    for (MixedLeafFieldComparator c : this.children) {
      if (this.bottomBracket == c.getBracketPriority()) {
        c.notifyNewBottom(bottom);
      }
    }

    if (oldBottomBracket != this.bottomBracket) {
      updateCompetitiveIterator();
    }
  }

  @Override
  public int compareBottom(int doc) throws IOException {
    for (int i = 0; i < this.children.length; ++i) {
      MixedLeafFieldComparator child = this.children[i];
      if (child.hasValue(doc)) {
        int bracket = child.getBracketPriority();
        if (this.bottomBracket > bracket) {
          return 1;
        } else if (bracket == this.bottomBracket) {
          return this.compareAllBottom(doc, i);
        } else {
          return -1;
        }
      }
    }
    // Value is missing, check bottom for null
    return Integer.compare(this.bottomBracket, this.nullBracket);
  }

  /**
   * Calls compareBottomToCurrent for all comparators in the same bracket as children[start].
   *
   * <p>This method should be logically equivalent to calling {@code
   * bottom.compareTo(selector.select(children[start:end]))} but with short-circuiting.
   *
   * <p>Note: {@code children[start].hasValue(doc)} must return true before calling this method.
   */
  private int compareAllBottom(int doc, int start) throws IOException {
    int mul = this.parent.selectMultiplier;
    int end = this.bracketEnds[start];
    @Var int cmp = mul * this.children[start].compareBottomToCurrent();

    // Terminate once we find the first competitive value even if it isn't the true min/max
    // For min selector: ∃child(bottom > child) ↔ bottom > min(children)
    // For max selector, we use the identity min(x, y) = -max(-x, -y)
    for (int i = start + 1; i < end && cmp <= 0; ++i) {
      if (!this.children[i].hasValue(doc)) {
        continue;
      }
      int next = mul * this.children[i].compareBottomToCurrent();
      cmp = Math.max(cmp, next);
    }

    return mul * cmp;
  }

  /**
   * Calls compareTopToCurrent for all comparators in the same bracket as children[start]. <br>
   * Note: children[start].hasValue(doc) must return true before calling this method.
   */
  private int compareAllTop(int doc, int start) throws IOException {
    int mul = this.parent.selectMultiplier;
    int end = this.bracketEnds[start];
    @Var int cmp = mul * this.children[start].compareTopToCurrent();

    // See rationale in compareAllBottom
    for (int i = start + 1; i < end && cmp <= 0; ++i) {
      if (!this.children[i].hasValue(doc)) {
        continue;
      }
      int next = mul * this.children[i].compareTopToCurrent();
      cmp = Math.max(cmp, next);
    }

    return mul * cmp;
  }

  /**
   * Read the appropriate min/max value from the bracket that matches {@code spanStart}.
   *
   * @param doc the internal Lucene document ID
   * @param spanStart The index into {@link #children} to read the first candidate value from. This
   *     value <b>MUST</b> be present. If there are multiple DocValue columns that compose the same
   *     type bracket, we will read each sequentially and take the min/max over all values within
   *     the bracket. This assumes that the {@code children} array is sorted by the bracket's sort
   *     priority. Note {@code spanStart} is not necessarily the first child in its bracket, but it
   *     is the first non-empty child of its bracket.
   */
  private BsonValue getValueFromSpan(int doc, int spanStart) throws IOException {
    int end = this.bracketEnds[spanStart];
    SortSelector selector = this.parent.options.selector();
    @Var BsonValue value = this.children[spanStart].getCurrentValue();

    for (int i = spanStart + 1; i < end; ++i) {
      if (this.children[i].hasValue(doc)) {
        BsonValue candidate = this.children[i].getCurrentValue();
        value =
            selector == SortSelector.MIN
                ? Comparators.min(value, candidate, SAME_BRACKET_COMPARATOR)
                : Comparators.max(value, candidate, SAME_BRACKET_COMPARATOR);
      }
    }
    return value;
  }

  @Override
  public int compareTop(int doc) throws IOException {
    int topBracket = this.parent.getTopBracket();

    for (int i = 0; i < this.children.length; ++i) {
      MixedLeafFieldComparator child = this.children[i];
      if (child.hasValue(doc)) {
        int bracket = child.getBracketPriority();
        if (topBracket > bracket) {
          return 1;
        } else if (bracket == topBracket) {
          return compareAllTop(doc, i);
        } else {
          return -1;
        }
      }
    }
    // Value is missing
    return Integer.compare(this.parent.getTopBracket(), this.nullBracket);
  }

  @Override
  public void copy(int slot, int doc) throws IOException {
    for (int i = 0; i < this.children.length; ++i) {
      if (this.children[i].hasValue(doc)) {
        this.parent.values[slot] = getValueFromSpan(doc, i);
        return;
      }
    }
    this.parent.values[slot] =
        this.parent.options.nullEmptySortPosition().getNullMissingSortValue();
  }

  /**
   * This method should be called upon initialization and whenever `bottom` or
   * `hitsThresholdReached` is updated.
   */
  void updateCompetitiveIterator() {
    if (!this.parent.hitsThresholdReached) {
      return;
    }
    if (this.parent.getTopBracket() == CompositeComparator.BRACKET_NOT_SET
        && this.bottomBracket == CompositeComparator.BRACKET_NOT_SET) {
      // We need at least a half-bounded range to prune values
      return;
    }

    int bestBracket = this.mostCompetitiveChild.getBracketPriority();
    // Case 1: Bottom is set and null-ish, so we don't have to consider missing values.
    if (this.bottomBracket == this.nullBracket) {
      if (this.orderMultiplier * bestBracket >= this.orderMultiplier * this.nullBracket) {
        // The best non-missing value is not better than null, so we can terminate
        this.competitiveIterator = DocIdSetIterator.empty();
      }
    }
  }

  @Nullable
  @Override
  public DocIdSetIterator competitiveIterator() {
    if (!this.parent.isSingleSort()) {
      // We currently prune out ties, which is invalid if we have tie-breaking sort fields
      return null;
    }
    // This method is called once per segment. We return a wrapper around a mutable reference
    // to `competitiveIterator` so that we can prune more aggressively as bottom is updated.
    return new DocIdSetIterator() {
      private int docID = CompositeLeafComparator.this.competitiveIterator.docID();

      @Override
      public int nextDoc() throws IOException {
        return advance(this.docID + 1);
      }

      @Override
      public int docID() {
        return this.docID;
      }

      @Override
      public long cost() {
        return CompositeLeafComparator.this.competitiveIterator.cost();
      }

      @Override
      public int advance(int target) throws IOException {
        return this.docID = CompositeLeafComparator.this.competitiveIterator.advance(target);
      }
    };
  }

  @Override
  public void setHitsThresholdReached() {
    this.parent.hitsThresholdReached = true;
    this.updateCompetitiveIterator();
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    // TODO(CLOUDP-213141): Implement for pruning, see NumericComparator implementation.
  }
}
