package com.xgen.mongot.index.lucene;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.lucene.LuceneSearchBatchProducer.IterValue;
import com.xgen.mongot.index.lucene.query.sort.mixed.SortUtil;
import com.xgen.mongot.index.query.sort.NullEmptySortPosition;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.bson.BsonValue;

/** A utility class that holds custom comparators used in BatchProducers. */
public class Comparators {

  private static final Comparator<ScoreDoc> TIEBREAKER =
      Comparator.<ScoreDoc>comparingInt(value -> value.shardIndex)
          .thenComparingInt(value -> value.doc);

  public static final Comparator<ScoreDoc> SCORE_DOC_RELEVANCE_COMPARATOR =
      Comparator.<ScoreDoc>comparingDouble(value -> -value.score).thenComparing(TIEBREAKER);

  /**
   * The comparator is used for topDocs sorted by score. It performs tiebreaking using shardIndex
   * and docId. It works for unpartitioned indexes because shardIndex is always -1 and defers to
   * docId.
   */
  public static final Comparator<IterValue> RELEVANCE_COMPARATOR =
      Comparator.comparing(IterValue::scoreDoc, SCORE_DOC_RELEVANCE_COMPARATOR);

  /**
   * The comparator used for topDocs sorted by custom lucene.search.Sort. The scoreDoc here must be
   * FieldScoreDoc.
   *
   * <p>Always tiebreaks using the partition index (`shardIndex`) first, followed by the docId.
   * Ensures ties within each partition are resolved in the original order (e.g., if `doc2` precedes
   * `doc3` in a partition, the merged result maintains this order). For unpartitioned indexes
   * (since `shardIndex` is always -1), ties are resolved solely by docId.
   */
  public static class CustomSortComparator implements Comparator<IterValue> {

    // Whether we should reverse the mqlMixedCompare() on the values for comparison.
    private final boolean[] reverseSort;

    public CustomSortComparator(SortField[] sortFields) {
      this.reverseSort = new boolean[sortFields.length];
      for (int comparatorIndex = 0; comparatorIndex < sortFields.length; comparatorIndex++) {
        SortField sortField = sortFields[comparatorIndex];
        // Note: Lucene uses reverse=True for ascending sort on score, which is the opposite of
        // how Lucene treats every other sort field.
        this.reverseSort[comparatorIndex] =
            sortField.getType() == SortField.Type.SCORE
                ? !sortField.getReverse()
                : sortField.getReverse();
      }
    }

    @Override
    public int compare(IterValue firstValue, IterValue secondValue) {
      List<BsonValue> first = firstValue.sortValues().get();
      List<BsonValue> second = secondValue.sortValues().get();
      checkState(
          first.size() == second.size(), "sortValues of first and second have different sizes.");
      checkState(
          first.size() == this.reverseSort.length,
          "Current SearchResultsIter.sortValues.size() != TopFieldDocs.sortFields.length");

      for (int i = 0; i < first.size(); i++) {
        // We use mqlMixedCompare() instead of the SortField.getComparator() here because the latter
        // one cannot handle the mixed type sort cases. Mixed type means the same field in different
        // index partitions can have different data types.
        int result =
            this.reverseSort[i]
                ? SortUtil.mqlMixedCompare(
                    second.get(i), first.get(i), NullEmptySortPosition.LOWEST)
                : SortUtil.mqlMixedCompare(
                    first.get(i), second.get(i), NullEmptySortPosition.LOWEST);
        if (result != 0) {
          return result;
        }
      }
      return TIEBREAKER.compare(firstValue.scoreDoc(), secondValue.scoreDoc());
    }
  }
}
