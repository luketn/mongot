package com.xgen.mongot.index.lucene.query.sort;

import java.util.Arrays;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Utility class for index sort operation.
 */
public class IndexSortUtils {

  /**
   * Extracts the index sort metadata from the first leaf reader.
   *
   * <p>
   * Since index sort is applied uniformly across all segments in index, checking the first
   * leaf reader is sufficient. If the first segment has no index sort, then no segment has
   * index sort.
   *
   * @param reader the index reader
   * @return the index sort metadata
   */
  public static Optional<Sort> extractFirstIndexSort(IndexReader reader) {
    if (reader.leaves().isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(reader.leaves().get(0).reader().getMetaData().getSort());
  }

  /**
   * Determines if a query sort can benefit from index sort optimization.
   *
   * <p>this method is the same as TopFieldCollector#canEarlyTerminateOnPrefix.
   */
  public static boolean canBenefitFromIndexSort(
      org.apache.lucene.search.Sort querySort,
      org.apache.lucene.search.Sort indexSort) {
    SortField[] querySortFields = querySort.getSort();
    SortField[] indexSortFields = indexSort.getSort();

    // early termination is possible if querySortFields is a prefix of indexSortFields
    if (querySortFields.length > indexSortFields.length) {
      return false;
    }

    return Arrays.asList(querySortFields).equals(
        Arrays.asList(indexSortFields).subList(0, querySortFields.length));
  }
}
