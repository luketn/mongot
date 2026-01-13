package com.xgen.mongot.index.query.sort;

import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;

/**
 * Defines strategy for selecting a representative value from a repeated field when sorting. Lucene
 * defines different selector types for numeric and string fields, so this class bundle's them
 * together so that {@link SortSelector#MAX} will refer to taking the max of either a string or
 * numeric field.
 */
public enum SortSelector {
  MIN(SortedNumericSelector.Type.MIN, SortedSetSelector.Type.MIN),
  MAX(SortedNumericSelector.Type.MAX, SortedSetSelector.Type.MAX);

  /** Sort selector applicable to NumericDocValue fields. */
  public final SortedNumericSelector.Type numericSelector;

  /** Sort selector applicable to SortedSetDocValue fields, such as strings or UUIDs. */
  public final SortedSetSelector.Type sortedSetSelector;

  SortSelector(
      SortedNumericSelector.Type numericSelector, SortedSetSelector.Type sortedSetSelector) {
    this.numericSelector = numericSelector;
    this.sortedSetSelector = sortedSetSelector;
  }
}
