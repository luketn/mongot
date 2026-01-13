package com.xgen.mongot.index.lucene.query.sort.common;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.truth.Truth;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import java.util.Optional;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.junit.Assert;
import org.junit.Test;

public class ExplainSortFieldTest {
  @Test
  public void testFieldComparator() {
    try (var unused =
        Explain.setup(
            Optional.of(Explain.Verbosity.EXECUTION_STATS),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()))) {
      SortFeatureExplainer explainer =
          Explain.getQueryInfo()
              .get()
              .getFeatureExplainer(
                  SortFeatureExplainer.class,
                  () ->
                      new SortFeatureExplainer(
                          SortSpecBuilder.builder()
                              .sortField(
                                  new MongotSortField(
                                      FieldPath.newRoot("foo"), UserFieldSortOptions.DEFAULT_DESC))
                              .buildSort(),
                          ImmutableSetMultimap.of(
                              FieldPath.newRoot("foo"), FieldName.TypeField.TOKEN)));

      SortField field = new SortField("foo", SortField.Type.STRING);
      ExplainSortField explainSortField = new ExplainSortField(field, explainer);
      Truth.assertThat(explainSortField.getComparator(10, Pruning.GREATER_THAN))
          .isInstanceOf(FieldComparatorExplainWrapper.class);
    }
  }

  @Test
  public void testSymmetricEqualityWithSortField() {
    SortField originalSortField = new SortField("foo", SortField.Type.STRING);
    ExplainSortField explainSortField = new ExplainSortField(originalSortField, null);

    Assert.assertEquals(originalSortField, explainSortField);
    Assert.assertEquals(explainSortField, originalSortField);
  }
}
