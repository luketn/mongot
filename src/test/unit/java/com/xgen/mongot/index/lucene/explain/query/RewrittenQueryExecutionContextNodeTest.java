package com.xgen.mongot.index.lucene.explain.query;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.testing.mongot.index.lucene.explain.query.RewrittenChildClausesBuilder;
import com.xgen.testing.mongot.index.lucene.explain.query.RewrittenQueryExecutionContextNodeBuilder;
import com.xgen.testing.mongot.index.lucene.explain.timing.TimingTestUtil;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class RewrittenQueryExecutionContextNodeTest {

  @Test
  public void testMergeTreesNoChildren() throws RewrittenQueryNodeException {
    var firstTimings = TimingTestUtil.randomTimings();
    var secondTimings = TimingTestUtil.randomTimings();

    var first =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(firstTimings)
            .clauseOccur(BooleanClause.Occur.MUST)
            .build();

    var second =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(secondTimings)
            .clauseOccur(BooleanClause.Occur.MUST)
            .build();

    var result = RewrittenQueryExecutionContextNode.mergeTrees(first, second);

    var mergedTimings = ExplainTimings.merge(firstTimings, secondTimings);
    var expected =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(mergedTimings)
            .clauseOccur(BooleanClause.Occur.MUST)
            .build();

    Truth.assertThat(result).isEqualTo(expected);
    // equals does not compare timing data
    Truth.assertThat(result.getTimings().stream().collect(ExplainTimings.toExplainTimingData()))
        .isEqualTo(mergedTimings.stream().collect(ExplainTimings.toExplainTimingData()));
  }

  @Test
  public void testMergeTreesWithChildrenSimple() throws RewrittenQueryNodeException {
    var firstChildren =
        RewrittenChildClausesBuilder.builder()
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("foo"))
                    .timings(TimingTestUtil.randomTimings())
                    .clauseOccur(BooleanClause.Occur.MUST)
                    .build())
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("bar"))
                    .timings(TimingTestUtil.randomTimings())
                    .clauseOccur(BooleanClause.Occur.SHOULD)
                    .build())
            .build();

    var first =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("baz"))
            .timings(TimingTestUtil.randomTimings())
            .childClauses(firstChildren)
            .build();

    var secondChildren =
        RewrittenChildClausesBuilder.builder()
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("foo"))
                    .timings(TimingTestUtil.randomTimings())
                    .clauseOccur(BooleanClause.Occur.MUST)
                    .build())
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("bar"))
                    .timings(TimingTestUtil.randomTimings())
                    .clauseOccur(BooleanClause.Occur.SHOULD)
                    .build())
            .build();

    var second =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("baz"))
            .timings(TimingTestUtil.randomTimings())
            .childClauses(secondChildren)
            .build();

    RewrittenQueryExecutionContextNode.mergeTrees(first, second);
  }

  @Test
  public void testMergeUnequalSimple() {
    var first =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("baz"))
            .timings(TimingTestUtil.randomTimings())
            .build();

    var second =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(TimingTestUtil.randomTimings())
            .build();

    Assert.assertThrows(
        RewrittenQueryNodeException.class,
        () -> RewrittenQueryExecutionContextNode.mergeTrees(first, second));
  }

  @Test
  public void testMergeUnequalNested() {
    var first =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .childClauses(
                RewrittenChildClausesBuilder.builder()
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("abc"))
                            .clauseOccur(BooleanClause.Occur.MUST)
                            .build())
                    .build())
            .build();

    var second =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .childClauses(
                RewrittenChildClausesBuilder.builder()
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("abc"))
                            .clauseOccur(BooleanClause.Occur.SHOULD)
                            .build())
                    .build())
            .build();

    Assert.assertThrows(
        RewrittenQueryNodeException.class,
        () -> RewrittenQueryExecutionContextNode.mergeTrees(first, second));
  }

  @Test
  public void testEquals() {
    var first =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(TimingTestUtil.randomTimings())
            .clauseOccur(BooleanClause.Occur.MUST)
            .build();

    var second =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(TimingTestUtil.randomTimings())
            .clauseOccur(BooleanClause.Occur.MUST)
            .build();

    Truth.assertThat(first).isEqualTo(second);
  }

  @Test
  public void testEqualsNested() {
    var first =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(TimingTestUtil.randomTimings())
            .childClauses(
                RewrittenChildClausesBuilder.builder()
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("bar"))
                            .clauseOccur(BooleanClause.Occur.MUST)
                            .build())
                    .build())
            .build();

    var second =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .timings(TimingTestUtil.randomTimings())
            .childClauses(
                RewrittenChildClausesBuilder.builder()
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("bar"))
                            .clauseOccur(BooleanClause.Occur.MUST)
                            .build())
                    .build())
            .build();

    Truth.assertThat(first).isEqualTo(second);
  }

  @Test
  public void testCreateCorrectlyRewritesChildren() throws RewrittenQueryNodeException {
    Function<BooleanClause.Occur, List<QueryVisitorQueryExecutionContextNode>>
        createDuplicateNodes =
            (occurArg) ->
                IntStream.range(0, 3)
                    .mapToObj(
                        unused ->
                            new QueryVisitorQueryExecutionContextNode(
                                termQuery("bar"),
                                Optional.of(occurArg),
                                TimingTestUtil.randomTimings(),
                                Optional.empty(),
                                Optional.empty()))
                    .toList();

    ChildClauses childClauses =
        new ChildClauses(
            List.copyOf(createDuplicateNodes.apply(BooleanClause.Occur.MUST)),
            List.copyOf(createDuplicateNodes.apply(BooleanClause.Occur.MUST_NOT)),
            List.copyOf(createDuplicateNodes.apply(BooleanClause.Occur.SHOULD)),
            List.copyOf(createDuplicateNodes.apply(BooleanClause.Occur.FILTER)));

    var result =
        RewrittenQueryExecutionContextNode.create(
            termQuery("foo"),
            TimingTestUtil.randomTimings(),
            Optional.of(childClauses),
            Optional.empty());

    var expected =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .childClauses(
                RewrittenChildClausesBuilder.builder()
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("bar"))
                            .clauseOccur(BooleanClause.Occur.MUST)
                            .build())
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("bar"))
                            .clauseOccur(BooleanClause.Occur.MUST_NOT)
                            .build())
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("bar"))
                            .clauseOccur(BooleanClause.Occur.SHOULD)
                            .build())
                    .child(
                        RewrittenQueryExecutionContextNodeBuilder.builder()
                            .query(termQuery("bar"))
                            .clauseOccur(BooleanClause.Occur.FILTER)
                            .build())
                    .build())
            .build();

    Truth.assertThat(result).isEqualTo(expected);
  }

  private static Query termQuery(String value) {
    return new TermQuery(new Term("path", value));
  }
}
