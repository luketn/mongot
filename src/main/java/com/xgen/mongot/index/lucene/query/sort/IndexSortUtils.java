package com.xgen.mongot.index.lucene.query.sort;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import java.util.Arrays;
import java.util.List;
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

  /**
   * Checks whether expanding the query sort with nullness fields would produce a sort that aligns
   * with the index sort. Splits the index sort into nullness fields and value fields, then verifies
   * that each query field's base field path (stripped of the {@code $type:} prefix) matches the
   * corresponding value field at the same position.
   *
   * <p>Comparison uses the base field path rather than the full Lucene field name so that
   * per-shard type differences (e.g., one shard has INT64 data, another has TOKEN data) do not
   * produce inconsistent expansion decisions across shards.
   *
   * @return {@code true} if the expanded query sort would be a valid prefix of the index sort
   */
  public static boolean expandedSortAlignsWithIndexSort(
      List<MongotSortField> queryFields,
      Sort indexSort) {
    SortField[] allIndexFields = indexSort.getSort();
    String nullnessPrefix = FieldName.MetaField.NULLNESS.getLuceneFieldName();

    List<String> indexValueNames = Arrays.stream(allIndexFields)
        .map(SortField::getField)
        .filter(name -> name == null || !name.startsWith(nullnessPrefix))
        .toList();

    if (queryFields.size() > indexValueNames.size()) {
      return false;
    }

    for (int i = 0; i < queryFields.size(); i++) {
      MongotSortField field = queryFields.get(i);
      String indexFieldName = indexValueNames.get(i);
      Optional<FieldName.TypeField> indexType =
          FieldName.TypeField.getTypeOf(indexFieldName);
      if (indexType.isEmpty()) {
        return false;
      }
      if (!indexType.get().stripPrefix(indexFieldName)
          .equals(field.field().toString())) {
        return false;
      }
    }
    return true;
  }
}
