package com.xgen.mongot.index.lucene.explain.explainers;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.profiler.ProfileDocIdSetIterator;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.lucene.explain.timing.TimingTestUtil;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.Test;

public class SortFeatureExplainerTest {
  @Test
  public void testFilteredFieldToTypes() {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                new MongotSortField(FieldPath.newRoot("foo"), UserFieldSortOptions.DEFAULT_ASC))
            .sortField(
                new MongotSortField(FieldPath.newRoot("bar"), UserFieldSortOptions.DEFAULT_ASC))
            .sortField(
                new MongotSortField(FieldPath.newRoot("baz"), UserFieldSortOptions.DEFAULT_DESC))
            .buildSort();

    SortFeatureExplainer explainer =
        new SortFeatureExplainer(
            sortSpec,
            ImmutableSetMultimap.of(
                FieldPath.newRoot("foo"),
                FieldName.TypeField.TOKEN,
                FieldPath.newRoot("foo"),
                FieldName.TypeField.NUMBER_INT64,
                FieldPath.newRoot("baz"),
                FieldName.TypeField.NUMBER_DOUBLE_V2,
                FieldPath.newRoot("abc"),
                FieldName.TypeField.DATE_V2));

    var filteredFieldToTypes = explainer.getFilteredFieldToTypes();
    var expected =
        ImmutableSetMultimap.of(
            FieldPath.newRoot("foo"),
            FieldName.TypeField.TOKEN,
            FieldPath.newRoot("foo"),
            FieldName.TypeField.NUMBER_INT64,
            FieldPath.newRoot("baz"),
            FieldName.TypeField.NUMBER_DOUBLE_V2);

    Truth.assertThat(filteredFieldToTypes).isEqualTo(expected);
  }

  @Test
  public void testAggregate() {
    SortSpec sortSpec =
        SortSpecBuilder.builder()
            .sortField(
                new MongotSortField(FieldPath.newRoot("foo"), UserFieldSortOptions.DEFAULT_DESC))
            .buildSort();

    SortFeatureExplainer explainer =
        new SortFeatureExplainer(
            sortSpec, ImmutableSetMultimap.of(FieldPath.newRoot("foo"), FieldName.TypeField.TOKEN));

    ExplainTimings first = TimingTestUtil.randomTimings();
    explainer.maybeAddCompetitiveIterator(
        Optional.of(ProfileDocIdSetIterator.create(DocIdSetIterator.empty(), first)));

    ExplainTimings second = TimingTestUtil.randomTimings();
    explainer.maybeAddCompetitiveIterator(
        Optional.of(ProfileDocIdSetIterator.create(DocIdSetIterator.empty(), second)));

    explainer.aggregate();

    List<ProfileDocIdSetIterator> aggregatedIterators = explainer.getAllCompetitiveIterators();
    Truth.assertThat(aggregatedIterators.size()).isEqualTo(1);
    ProfileDocIdSetIterator result = aggregatedIterators.getFirst();

    Truth.assertThat(
            result.getExplainTimings().stream().collect(ExplainTimings.toExplainTimingData()))
        .isEqualTo(
            ExplainTimings.merge(first, second).stream()
                .collect(ExplainTimings.toExplainTimingData()));
  }
}
