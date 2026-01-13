package com.xgen.mongot.index.lucene.explain.information.creator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.MatchAllDocsQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.WrappedKnnQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimingBreakdown;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.query.custom.WrappedQuery;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.timers.TimingData;
import com.xgen.testing.mongot.index.lucene.explain.information.ExplainInformationTestUtil;
import com.xgen.testing.mongot.index.lucene.explain.query.MockQueryExecutionContextNode;
import com.xgen.testing.mongot.index.lucene.explain.timing.TimingTestUtil;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.collections4.Equator;
import org.apache.commons.collections4.functors.DefaultEquator;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class QueryExplainInformationCreatorTest {
  static class EquatorSuite {
    final Equator<LuceneQuerySpecification> queryEquator;
    final Equator<ExplainTimingBreakdown> timingEquator;

    EquatorSuite(
        Equator<LuceneQuerySpecification> queryEquator,
        Equator<ExplainTimingBreakdown> timingEquator) {
      this.queryEquator = queryEquator;
      this.timingEquator = timingEquator;
    }
  }

  private static Map<ExplainTimings.Type, TimingData> combineTimings(
      Stream<ExplainTimings> timings) {
    return timings.flatMap(ExplainTimings::stream).collect(ExplainTimings.toExplainTimingData());
  }

  @Test
  public void testFromNoOperator() {
    Query nodeQuery = new MatchAllDocsQuery();
    ExplainTimings nodeTimings = TimingTestUtil.randomTimings();
    Optional<QueryChildren<QueryExecutionContextNode>> children = Optional.empty();

    QueryExecutionContextNode node =
        new MockQueryExecutionContextNode(nodeQuery, nodeTimings, children);
    Optional<FieldPath> operatorPath = Optional.empty();
    Explain.Verbosity verbosity = Explain.Verbosity.ALL_PLANS_EXECUTION;

    QueryExplainInformation result =
        QueryExplainInformationCreator.from(node, operatorPath, Optional.empty(), verbosity);

    LuceneQuerySpecification expectedQuerySpec = new MatchAllDocsQuerySpec();

    QueryExplainInformation expected =
        new QueryExplainInformation(
            Optional.empty(),
            LuceneQuerySpecification.Type.MATCH_ALL_DOCS_QUERY,
            Optional.empty(),
            expectedQuerySpec,
            Optional.of(
                ExplainTimingBreakdown.fromExecutionStats(nodeTimings.extractTimingData())));

    EquatorSuite equatorSuite =
        new EquatorSuite(
            typeMatchingQuerySpecEquator(LuceneQuerySpecification.Type.MATCH_ALL_DOCS_QUERY),
            DefaultEquator.defaultEquator());

    assertEquals("non-wrapped query should be as expected", expected, result, equatorSuite);
  }

  @Test
  public void testFromWrappedQueryNotCompound() {
    Query nodeQuery = new TermQuery(new Term("fooPath", "barQuery"));
    ExplainTimings nodeTimings = TimingTestUtil.randomTimings();
    QueryExecutionContextNode node =
        new MockQueryExecutionContextNode(nodeQuery, nodeTimings, Optional.empty());

    Optional<FieldPath> operatorPath = Optional.empty();
    Query wrappedQuery = new WrappedQuery(nodeQuery, operatorPath);
    ExplainTimings wrappedTimings = TimingTestUtil.randomTimings();
    Optional<QueryChildren<QueryExecutionContextNode>> children =
        Optional.of(getWrappedChild(List.of(node), null, null, null));
    QueryExecutionContextNode wrappedNode =
        new MockQueryExecutionContextNode(wrappedQuery, wrappedTimings, children);
    Explain.Verbosity verbosity = Explain.Verbosity.ALL_PLANS_EXECUTION;

    QueryExplainInformation result =
        QueryExplainInformationCreator.fromNode(wrappedNode, verbosity);

    LuceneQuerySpecification expectedQuerySpec = mock(LuceneQuerySpecification.class);
    when(expectedQuerySpec.getType()).thenReturn(LuceneQuerySpecification.Type.TERM_QUERY);

    var expectedTimings = combineTimings(Stream.of(wrappedTimings, nodeTimings));
    QueryExplainInformation expected =
        new QueryExplainInformation(
            Optional.empty(),
            LuceneQuerySpecification.Type.TERM_QUERY,
            Optional.empty(),
            expectedQuerySpec,
            Optional.of(ExplainTimingBreakdown.fromExecutionStats(expectedTimings)));

    EquatorSuite equatorSuite =
        new EquatorSuite(
            typeMatchingQuerySpecEquator(LuceneQuerySpecification.Type.TERM_QUERY),
            DefaultEquator.defaultEquator());

    assertEquals("non-wrapped query should be as expected", expected, result, equatorSuite);
  }

  @Test
  public void testFromWrappedCompound() {
    Query termQuery = new TermQuery(new Term("fooPath", "barQuery"));
    ExplainTimings termTimings = TimingTestUtil.randomTimings();
    QueryExecutionContextNode termNode =
        new MockQueryExecutionContextNode(termQuery, termTimings, Optional.empty());

    Optional<FieldPath> textPath = Optional.of(FieldPath.parse("compound.must[0]"));
    Query textWrappedQuery = new WrappedQuery(termQuery, textPath);
    ExplainTimings textWrappedTimings = TimingTestUtil.randomTimings();
    Optional<QueryChildren<QueryExecutionContextNode>> textWrappedChildren =
        Optional.of(getWrappedChild(List.of(termNode), null, null, null));
    QueryExecutionContextNode textNode =
        new MockQueryExecutionContextNode(
            textWrappedQuery, textWrappedTimings, textWrappedChildren);

    Query booleanQuery =
        new BooleanQuery.Builder().add(textWrappedQuery, BooleanClause.Occur.MUST).build();
    ExplainTimings booleanTimings = TimingTestUtil.randomTimings();
    Optional<QueryChildren<QueryExecutionContextNode>> booleanChildren =
        Optional.of(getWrappedChild(List.of(textNode), null, null, null));
    QueryExecutionContextNode booleanNode =
        new MockQueryExecutionContextNode(booleanQuery, booleanTimings, booleanChildren);

    Optional<FieldPath> compoundPath = Optional.empty();
    Query compoundWrappedQuery = new WrappedQuery(booleanQuery, compoundPath);
    ExplainTimings compoundTimings = TimingTestUtil.randomTimings();
    Optional<QueryChildren<QueryExecutionContextNode>> compoundChildren =
        Optional.of(getWrappedChild(List.of(booleanNode), null, null, null));
    QueryExecutionContextNode rootCompoundNode =
        new MockQueryExecutionContextNode(compoundWrappedQuery, compoundTimings, compoundChildren);

    Explain.Verbosity verbosity = Explain.Verbosity.ALL_PLANS_EXECUTION;
    QueryExplainInformation result =
        QueryExplainInformationCreator.fromNode(rootCompoundNode, verbosity);

    LuceneQuerySpecification expectedQuerySpec =
        LuceneQuerySpecificationCreator.querySpecFor(booleanNode, verbosity);

    var compoundExpectedTimings = combineTimings(Stream.of(compoundTimings, booleanTimings));
    QueryExplainInformation expected =
        new QueryExplainInformation(
            Optional.empty(),
            LuceneQuerySpecification.Type.BOOLEAN_QUERY,
            Optional.empty(),
            expectedQuerySpec,
            Optional.of(ExplainTimingBreakdown.fromExecutionStats(compoundExpectedTimings)));

    assertEquals(
        "non-wrapped query should be as expected",
        expected,
        result,
        ExplainInfoExactEquator.INSTANCE);
  }

  @Test
  public void testFromWrappedKnn() {
    Query knnFloatQuery = new KnnFloatVectorQuery("path", new float[] {1, 2, 3}, 100);
    ExplainTimings knnFloatTimings = TimingTestUtil.randomTimings();
    QueryExecutionContextNode knnFloatNode =
        new MockQueryExecutionContextNode(knnFloatQuery, knnFloatTimings, Optional.empty());

    Query matchNoDocsQuery = new MatchNoDocsQuery();
    ExplainTimings matchNoDocsTimings = TimingTestUtil.randomTimings();
    QueryExecutionContextNode matchNoDocsNode =
        new MockQueryExecutionContextNode(matchNoDocsQuery, matchNoDocsTimings, Optional.empty());

    Query termQuery = new TermQuery(new Term("fooPath", "barQuery"));
    Query booleanFilterQuery =
        new BooleanQuery.Builder().add(termQuery, BooleanClause.Occur.MUST).build();
    ExplainTimings booleanTimings = TimingTestUtil.randomTimings();
    QueryExecutionContextNode booleanNode =
        new MockQueryExecutionContextNode(booleanFilterQuery, booleanTimings, Optional.empty());

    Query wrappedKnnQuery = new WrappedKnnQuery(knnFloatQuery);
    ExplainTimings wrappedKnnTimings = TimingTestUtil.randomTimings();
    QueryExecutionContextNode wrappedKnnNode =
        new MockQueryExecutionContextNode(
            wrappedKnnQuery,
            wrappedKnnTimings,
            Optional.of(
                getWrappedChild(
                    List.of(knnFloatNode, matchNoDocsNode), null, null, List.of(booleanNode))));

    Explain.Verbosity verbosity = Explain.Verbosity.ALL_PLANS_EXECUTION;
    QueryExplainInformation filter =
        QueryExplainInformationCreator.fromNode(booleanNode, verbosity);
    QueryExplainInformation knnFloat =
        QueryExplainInformationCreator.fromNode(knnFloatNode, verbosity);
    QueryExplainInformation matchNoDocs =
        QueryExplainInformationCreator.fromNode(matchNoDocsNode, verbosity);
    LuceneQuerySpecification expectedSpec =
        new WrappedKnnQuerySpec(List.of(knnFloat, matchNoDocs), Optional.of(filter));

    var finalTimings =
        combineTimings(
            Stream.of(knnFloatTimings, matchNoDocsTimings, booleanTimings, wrappedKnnTimings));

    QueryExplainInformation expected =
        new QueryExplainInformation(
            Optional.empty(),
            LuceneQuerySpecification.Type.WRAPPED_KNN_QUERY,
            Optional.empty(),
            expectedSpec,
            Optional.of(ExplainTimingBreakdown.fromExecutionStats(finalTimings)));

    QueryExplainInformation result =
        QueryExplainInformationCreator.fromNode(wrappedKnnNode, verbosity);

    assertEquals(
        "non-wrapped query should be as expected",
        expected,
        result,
        ExplainInfoExactEquator.INSTANCE);
  }

  private static void assertEquals(
      String message,
      QueryExplainInformation expected,
      QueryExplainInformation result,
      EquatorSuite equatorSuite) {
    Assert.assertTrue(
        message, result.equals(expected, equatorSuite.queryEquator, equatorSuite.timingEquator));
  }

  private static void assertEquals(
      String message,
      QueryExplainInformation expected,
      QueryExplainInformation result,
      Equator<QueryExplainInformation> equator) {
    Assert.assertTrue(message, equator.equate(expected, result));
  }

  static class ExplainInfoExactEquator implements Equator<QueryExplainInformation> {
    static final Equator<QueryExplainInformation> INSTANCE = new ExplainInfoExactEquator();

    @Override
    public boolean equate(QueryExplainInformation o1, QueryExplainInformation o2) {
      return o1.equals(o2, QuerySpecExactEquator.INSTANCE, DefaultEquator.defaultEquator());
    }

    @Override
    public int hash(QueryExplainInformation o) {
      return 0;
    }
  }

  static class QuerySpecExactEquator implements Equator<LuceneQuerySpecification> {
    static final Equator<LuceneQuerySpecification> INSTANCE = new QuerySpecExactEquator();

    @Override
    public boolean equate(LuceneQuerySpecification o1, LuceneQuerySpecification o2) {
      return o1.equals(
          o2, ExplainInfoExactEquator.INSTANCE, ExplainInformationTestUtil.totalOrderComparator());
    }

    @Override
    public int hash(LuceneQuerySpecification o) {
      return 0;
    }
  }

  private static Equator<LuceneQuerySpecification> typeMatchingQuerySpecEquator(
      LuceneQuerySpecification.Type type) {
    return new Equator<>() {
      @Override
      public boolean equate(LuceneQuerySpecification o1, LuceneQuerySpecification o2) {
        return o1.getType() == type && o2.getType() == type;
      }

      @Override
      public int hash(LuceneQuerySpecification o) {
        return Objects.hash(o);
      }
    };
  }

  private static QueryChildren<QueryExecutionContextNode> getWrappedChild(
      List<QueryExecutionContextNode> must,
      List<QueryExecutionContextNode> mustNot,
      List<QueryExecutionContextNode> should,
      List<QueryExecutionContextNode> filter) {
    return new QueryChildren<>() {
      @Override
      public List<QueryExecutionContextNode> must() {
        return must;
      }

      @Override
      public List<QueryExecutionContextNode> mustNot() {
        return mustNot;
      }

      @Override
      public List<QueryExecutionContextNode> should() {
        return should;
      }

      @Override
      public List<QueryExecutionContextNode> filter() {
        return filter;
      }

      @Override
      public void addClause(QueryExecutionContextNode child, BooleanClause.Occur occur) {}

      @Override
      public Optional<BooleanClause.Occur> occurFor(QueryExecutionContextNode child) {
        return Optional.empty();
      }

      @Override
      public void removeChild(QueryExecutionContextNode child) {}
    };
  }
}
