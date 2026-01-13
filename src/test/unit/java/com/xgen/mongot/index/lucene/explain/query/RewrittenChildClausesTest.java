package com.xgen.mongot.index.lucene.explain.query;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.testing.mongot.index.lucene.explain.query.RewrittenChildClausesBuilder;
import com.xgen.testing.mongot.index.lucene.explain.query.RewrittenQueryExecutionContextNodeBuilder;
import com.xgen.testing.mongot.index.lucene.explain.timing.TimingTestUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class RewrittenChildClausesTest {

  @Test
  public void testDeduplicateIdenticalQueriesSimple() throws RewrittenQueryNodeException {
    List<ExplainTimings> timings = new ArrayList<>();
    List<QueryVisitorQueryExecutionContextNode> nodes = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      var currTimings = TimingTestUtil.randomTimings();
      nodes.add(
          new QueryVisitorQueryExecutionContextNode(
              termQuery("foo"), Optional.empty(), currTimings, Optional.empty(), Optional.empty()));
      timings.add(currTimings);
    }

    var result = RewrittenChildClauses.rewriteAndDeduplicateIdenticalQueries(nodes);
    Truth.assertThat(result.size()).isEqualTo(1);
    Truth.assertThat(result)
        .containsExactly(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(termQuery("foo"))
                .timings(TimingTestUtil.randomTimings())
                .build());
    TimingTestUtil.assertTimings(
        result.stream().findFirst().get().getTimings(),
        timings.stream().reduce(ExplainTimings.builder().build(), ExplainTimings::merge));
  }

  @Test
  public void testDeduplicateIdenticalQueriesNested() throws RewrittenQueryNodeException {
    List<QueryVisitorQueryExecutionContextNode> firstNodes = new ArrayList<>();
    List<QueryVisitorQueryExecutionContextNode> secondNodes = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      var currTimings = TimingTestUtil.randomTimings();
      var childClauses =
          new ChildClauses(
              List.of(
                  new QueryVisitorQueryExecutionContextNode(
                      termQuery("baz"),
                      Optional.of(BooleanClause.Occur.MUST),
                      TimingTestUtil.randomTimings(),
                      Optional.empty(),
                      Optional.empty())),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList());

      firstNodes.add(
          new QueryVisitorQueryExecutionContextNode(
              termQuery("foo"),
              Optional.empty(),
              currTimings,
              Optional.empty(),
              Optional.of(childClauses)));
    }

    for (int i = 0; i < 5; i++) {
      var currTimings = TimingTestUtil.randomTimings();
      var childClauses =
          new ChildClauses(
              List.of(
                  new QueryVisitorQueryExecutionContextNode(
                      termQuery("bar"),
                      Optional.of(BooleanClause.Occur.MUST),
                      TimingTestUtil.randomTimings(),
                      Optional.empty(),
                      Optional.empty())),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList());

      secondNodes.add(
          new QueryVisitorQueryExecutionContextNode(
              termQuery("foo"),
              Optional.empty(),
              currTimings,
              Optional.empty(),
              Optional.of(childClauses)));
    }

    var allNodes = Stream.concat(firstNodes.stream(), secondNodes.stream()).toList();
    var result = RewrittenChildClauses.rewriteAndDeduplicateIdenticalQueries(allNodes);
    Truth.assertThat(result.size()).isEqualTo(2);
    Truth.assertThat(result)
        .containsExactly(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(termQuery("foo"))
                .timings(TimingTestUtil.randomTimings())
                .childClauses(
                    RewrittenChildClausesBuilder.builder()
                        .child(
                            RewrittenQueryExecutionContextNodeBuilder.builder()
                                .query(termQuery("baz"))
                                .clauseOccur(BooleanClause.Occur.MUST)
                                .timings(TimingTestUtil.randomTimings())
                                .build())
                        .build())
                .build(),
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(termQuery("foo"))
                .timings(TimingTestUtil.randomTimings())
                .childClauses(
                    RewrittenChildClausesBuilder.builder()
                        .child(
                            RewrittenQueryExecutionContextNodeBuilder.builder()
                                .query(termQuery("bar"))
                                .clauseOccur(BooleanClause.Occur.MUST)
                                .timings(TimingTestUtil.randomTimings())
                                .build())
                        .build())
                .build());
  }

  @Test
  public void testCreate() throws RewrittenQueryNodeException {
    Function<BooleanClause.Occur, List<QueryVisitorQueryExecutionContextNode>>
        createDuplicateNodes =
            (occurArg) ->
                IntStream.range(0, 3)
                    .mapToObj(
                        unused ->
                            new QueryVisitorQueryExecutionContextNode(
                                termQuery("foo"),
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

    var result = RewrittenChildClauses.create(childClauses);

    Truth.assertThat(result)
        .isEqualTo(
            RewrittenChildClausesBuilder.builder()
                .child(
                    RewrittenQueryExecutionContextNodeBuilder.builder()
                        .query(termQuery("foo"))
                        .clauseOccur(BooleanClause.Occur.MUST)
                        .build())
                .child(
                    RewrittenQueryExecutionContextNodeBuilder.builder()
                        .query(termQuery("foo"))
                        .clauseOccur(BooleanClause.Occur.MUST_NOT)
                        .build())
                .child(
                    RewrittenQueryExecutionContextNodeBuilder.builder()
                        .query(termQuery("foo"))
                        .clauseOccur(BooleanClause.Occur.SHOULD)
                        .build())
                .child(
                    RewrittenQueryExecutionContextNodeBuilder.builder()
                        .query(termQuery("foo"))
                        .clauseOccur(BooleanClause.Occur.FILTER)
                        .build())
                .build());
  }

  @Test
  public void testFindCorrespondingChild() throws RewrittenQueryNodeException {
    Set<RewrittenQueryExecutionContextNode> nodes =
        Stream.of("foo", "bar", "baz")
            .map(
                term ->
                    RewrittenQueryExecutionContextNodeBuilder.builder()
                        .query(termQuery(term))
                        .build())
            .collect(Collectors.toSet());

    RewrittenQueryExecutionContextNode present =
        RewrittenQueryExecutionContextNodeBuilder.builder().query(termQuery("foo")).build();

    RewrittenQueryExecutionContextNode notPresent =
        RewrittenQueryExecutionContextNodeBuilder.builder()
            .query(termQuery("foo"))
            .clauseOccur(BooleanClause.Occur.FILTER)
            .build();

    Truth.assertThat(RewrittenChildClauses.findCorrespondingChild(present, nodes))
        .isEqualTo(present);

    Assert.assertThrows(
        RewrittenQueryNodeException.class,
        () -> RewrittenChildClauses.findCorrespondingChild(notPresent, nodes));
  }

  @Test
  public void testFindCorrespondingChildNested() throws RewrittenQueryNodeException {
    Set<RewrittenQueryExecutionContextNode> nodes =
        Stream.of("foo", "bar", "baz")
            .map(
                term ->
                    RewrittenQueryExecutionContextNodeBuilder.builder()
                        .query(termQuery(term))
                        .build())
            .collect(Collectors.toSet());

    nodes.add(
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
            .build());

    RewrittenQueryExecutionContextNode toFind =
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

    Truth.assertThat(RewrittenChildClauses.findCorrespondingChild(toFind, nodes)).isEqualTo(toFind);
  }

  @Test
  public void testMergeClausesNotPresent() throws RewrittenQueryNodeException {
    Truth.assertThat(RewrittenChildClauses.mergeClauses(Optional.empty(), Optional.empty()))
        .isEmpty();
  }

  @Test
  public void testMergeClausesSingleClause() {
    Assert.assertThrows(
        RewrittenQueryNodeException.class,
        () ->
            RewrittenChildClauses.mergeClauses(
                Optional.empty(), Optional.of(RewrittenChildClausesBuilder.builder().build())));
  }

  @Test
  public void testMergeClausesSimple() throws RewrittenQueryNodeException {
    RewrittenChildClausesBuilder builder =
        RewrittenChildClausesBuilder.builder()
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("foo"))
                    .clauseOccur(BooleanClause.Occur.MUST)
                    .build())
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("bar"))
                    .clauseOccur(BooleanClause.Occur.SHOULD)
                    .build());

    var result =
        RewrittenChildClauses.mergeClauses(
            Optional.of(builder.build()), Optional.of(builder.build()));
    Truth.assertThat(result).isPresent();
    Truth.assertThat(result.get().must())
        .containsExactly(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(termQuery("foo"))
                .clauseOccur(BooleanClause.Occur.MUST)
                .build());
    Truth.assertThat(result.get().should())
        .containsExactly(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(termQuery("bar"))
                .clauseOccur(BooleanClause.Occur.SHOULD)
                .build());
    Truth.assertThat(result.get().filter()).isEmpty();
    Truth.assertThat(result.get().mustNot()).isEmpty();
  }

  @Test
  public void testMergeDuplicateQueryMultipleClauseSet() throws RewrittenQueryNodeException {
    RewrittenChildClausesBuilder builder =
        RewrittenChildClausesBuilder.builder()
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("foo"))
                    .clauseOccur(BooleanClause.Occur.MUST)
                    .childClauses(
                        RewrittenChildClausesBuilder.builder()
                            .child(
                                RewrittenQueryExecutionContextNodeBuilder.builder()
                                    .query(termQuery("abc"))
                                    .clauseOccur(BooleanClause.Occur.MUST)
                                    .build())
                            .build())
                    .build())
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("foo"))
                    .clauseOccur(BooleanClause.Occur.SHOULD)
                    .childClauses(
                        RewrittenChildClausesBuilder.builder()
                            .child(
                                RewrittenQueryExecutionContextNodeBuilder.builder()
                                    .query(termQuery("abc"))
                                    .clauseOccur(BooleanClause.Occur.MUST)
                                    .build())
                            .build())
                    .build());

    var result =
        RewrittenChildClauses.mergeClauses(
            Optional.of(builder.build()), Optional.of(builder.build()));
    Truth.assertThat(result).isPresent();
    Truth.assertThat(result.get().must())
        .containsExactly(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(termQuery("foo"))
                .clauseOccur(BooleanClause.Occur.MUST)
                .childClauses(
                    RewrittenChildClausesBuilder.builder()
                        .child(
                            RewrittenQueryExecutionContextNodeBuilder.builder()
                                .query(termQuery("abc"))
                                .clauseOccur(BooleanClause.Occur.MUST)
                                .build())
                        .build())
                .build());
    Truth.assertThat(result.get().should())
        .containsExactly(
            RewrittenQueryExecutionContextNodeBuilder.builder()
                .query(termQuery("foo"))
                .clauseOccur(BooleanClause.Occur.SHOULD)
                .childClauses(
                    RewrittenChildClausesBuilder.builder()
                        .child(
                            RewrittenQueryExecutionContextNodeBuilder.builder()
                                .query(termQuery("abc"))
                                .clauseOccur(BooleanClause.Occur.MUST)
                                .build())
                        .build())
                .build());
    Truth.assertThat(result.get().filter()).isEmpty();
    Truth.assertThat(result.get().mustNot()).isEmpty();
  }

  @Test
  public void testMergeDuplicateQueryUnequalClauses() {
    RewrittenChildClauses first =
        RewrittenChildClausesBuilder.builder()
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("foo"))
                    .clauseOccur(BooleanClause.Occur.MUST)
                    .build())
            .build();

    RewrittenChildClauses second =
        RewrittenChildClausesBuilder.builder()
            .child(
                RewrittenQueryExecutionContextNodeBuilder.builder()
                    .query(termQuery("foo"))
                    .clauseOccur(BooleanClause.Occur.FILTER)
                    .build())
            .build();

    Assert.assertThrows(
        RewrittenQueryNodeException.class,
        () -> RewrittenChildClauses.mergeClauses(Optional.of(first), Optional.of(second)));
  }

  private static Query termQuery(String value) {
    return new TermQuery(new Term("path", value));
  }
}
