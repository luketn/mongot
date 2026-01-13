package com.xgen.mongot.index.lucene.query.sort.common;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.truth.Truth;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.explainers.SortFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.sort.MongotSortField;
import com.xgen.mongot.index.query.sort.UserFieldSortOptions;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.query.sort.SortSpecBuilder;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FieldComparatorExplainWrapperTest {

  private static Scope explainScope;

  private static SortFeatureExplainer explainer;
  private static FieldComparatorExplainWrapper<Long> explainWrapper;
  private static Directory directory;
  private static IndexReader reader;
  private static IndexWriter writer;

  @Before
  @SuppressWarnings("MustBeClosedChecker")
  public void setup() throws IOException {
    explainScope =
        Explain.setup(
            Optional.of(Explain.Verbosity.EXECUTION_STATS),
            Optional.of(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue()));

    var fieldPath = FieldPath.newRoot("foo");
    explainer =
        Explain.getQueryInfo()
            .get()
            .getFeatureExplainer(
                SortFeatureExplainer.class,
                () ->
                    new SortFeatureExplainer(
                        SortSpecBuilder.builder()
                            .sortField(
                                new MongotSortField(fieldPath, UserFieldSortOptions.DEFAULT_ASC))
                            .buildSort(),
                        ImmutableSetMultimap.of(fieldPath, FieldName.TypeField.NUMBER_INT64_V2)));
    LongComparator comparator = new LongComparator(10, "foo", -1L, false, Pruning.GREATER_THAN);
    explainWrapper = new FieldComparatorExplainWrapper<>(comparator, explainer);

    TemporaryFolder temporaryFolder = TestUtils.getTempFolder();
    directory = new MMapDirectory(temporaryFolder.getRoot().toPath());
    writer =
        new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    createLeafSegments();
    reader = DirectoryReader.open(directory);
  }

  @After
  public void teardown() throws IOException {
    reader.close();
    writer.close();
    directory.close();
    explainScope.close();
  }

  @Test
  public void testCompareTop() throws IOException {
    LeafFieldComparator leafComparator =
        explainWrapper.getLeafComparator(reader.leaves().getFirst().reader().getContext());

    leafComparator.compareTop(0);

    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.COMPARE_TOP).getElapsedNanos())
        .isGreaterThan(0);
    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.COMPARE_TOP).getInvocationCount())
        .isEqualTo(1);
  }

  @Test
  public void testSetBottom() throws IOException {
    LeafFieldComparator leafComparator =
        explainWrapper.getLeafComparator(reader.leaves().getFirst().reader().getContext());

    leafComparator.setBottom(0);

    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.SET_BOTTOM).getElapsedNanos())
        .isGreaterThan(0);
    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.SET_BOTTOM).getInvocationCount())
        .isEqualTo(1);
  }

  @Test
  public void testCompareBottom() throws IOException {
    LeafFieldComparator leafComparator =
        explainWrapper.getLeafComparator(reader.leaves().getFirst().reader().getContext());

    leafComparator.compareBottom(0);

    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.COMPARE_BOTTOM).getElapsedNanos())
        .isGreaterThan(0);
    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.COMPARE_BOTTOM).getInvocationCount())
        .isEqualTo(1);
  }

  @Test
  public void testSetScorer() throws IOException {
    LeafFieldComparator leafComparator =
        explainWrapper.getLeafComparator(reader.leaves().getFirst().reader().getContext());

    Query query = new TermQuery(new Term("bar", "a"));
    Weight weight = query.createWeight(new IndexSearcher(reader), ScoreMode.COMPLETE, 1.0f);
    var scorer = weight.scorer(reader.leaves().getFirst());
    leafComparator.setScorer(scorer);

    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.SET_SCORER).getElapsedNanos())
        .isGreaterThan(0);
    Truth.assertThat(
            explainer.getTimings().ofType(ExplainTimings.Type.SET_SCORER).getInvocationCount())
        .isEqualTo(1);
  }

  @Test
  public void testGetCompetitiveIterator() throws IOException {
    LeafFieldComparator leafComparator =
        explainWrapper.getLeafComparator(reader.leaves().getFirst().reader().getContext());

    leafComparator.competitiveIterator();

    Truth.assertThat(
            explainer
                .getTimings()
                .ofType(ExplainTimings.Type.COMPETITIVE_ITERATOR)
                .getElapsedNanos())
        .isGreaterThan(0);
    Truth.assertThat(
            explainer
                .getTimings()
                .ofType(ExplainTimings.Type.COMPETITIVE_ITERATOR)
                .getInvocationCount())
        .isEqualTo(1);
  }

  @Test
  public void testSetHitsThresholdReached() throws IOException {
    LeafFieldComparator leafComparator =
        explainWrapper.getLeafComparator(reader.leaves().getFirst().reader().getContext());

    leafComparator.setHitsThresholdReached();

    Truth.assertThat(
            explainer
                .getTimings()
                .ofType(ExplainTimings.Type.SET_HITS_THRESHOLD_REACHED)
                .getElapsedNanos())
        .isGreaterThan(0);
    Truth.assertThat(
            explainer
                .getTimings()
                .ofType(ExplainTimings.Type.SET_HITS_THRESHOLD_REACHED)
                .getInvocationCount())
        .isEqualTo(1);
  }

  private void createLeafSegments() throws IOException {
    Document doc1 = new Document();
    doc1.add(new NumericDocValuesField("foo", 5));
    doc1.add(new LongPoint("foo", 5));
    writer.addDocument(doc1);
    writer.commit();

    Document doc2 = new Document();
    doc2.add(new NumericDocValuesField("foo", 6));
    doc2.add(new LongPoint("foo", 6));
    writer.addDocument(doc2);
    writer.commit();
  }
}
