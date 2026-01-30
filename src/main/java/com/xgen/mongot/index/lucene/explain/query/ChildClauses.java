package com.xgen.mongot.index.lucene.explain.query;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.search.BooleanClause;

public class ChildClauses implements QueryChildren<QueryVisitorQueryExecutionContextNode> {
  private final List<QueryVisitorQueryExecutionContextNode> mustChildren;
  private final List<QueryVisitorQueryExecutionContextNode> mustNotChildren;
  private final List<QueryVisitorQueryExecutionContextNode> shouldChildren;
  private final List<QueryVisitorQueryExecutionContextNode> filterChildren;

  public ChildClauses() {
    this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }

  @VisibleForTesting
  ChildClauses(
      List<QueryVisitorQueryExecutionContextNode> mustChildren,
      List<QueryVisitorQueryExecutionContextNode> mustNotChildren,
      List<QueryVisitorQueryExecutionContextNode> shouldChildren,
      List<QueryVisitorQueryExecutionContextNode> filterChildren) {
    this.mustChildren = mustChildren;
    this.mustNotChildren = mustNotChildren;
    this.shouldChildren = shouldChildren;
    this.filterChildren = filterChildren;
  }

  @Override
  public void addClause(QueryVisitorQueryExecutionContextNode child, BooleanClause.Occur occur) {
    switch (occur) {
      case MUST -> this.mustChildren.add(child);
      case SHOULD -> this.shouldChildren.add(child);
      case MUST_NOT -> this.mustNotChildren.add(child);
      case FILTER -> this.filterChildren.add(child);
    }
  }

  @Override
  public List<QueryVisitorQueryExecutionContextNode> must() {
    return this.mustChildren;
  }

  @Override
  public List<QueryVisitorQueryExecutionContextNode> mustNot() {
    return this.mustNotChildren;
  }

  @Override
  public List<QueryVisitorQueryExecutionContextNode> should() {
    return this.shouldChildren;
  }

  @Override
  public List<QueryVisitorQueryExecutionContextNode> filter() {
    return this.filterChildren;
  }

  @Override
  public Optional<BooleanClause.Occur> occurFor(QueryVisitorQueryExecutionContextNode child) {
    BiFunction<
            List<? extends QueryExecutionContextNode>,
            BooleanClause.Occur,
            Optional<BooleanClause.Occur>>
        clauseOccurIfChildPresent =
            (clauseNodes, occur) ->
                clauseNodes.contains(child) ? Optional.of(occur) : Optional.empty();

    Optional<BooleanClause.Occur> must =
        clauseOccurIfChildPresent.apply(must(), BooleanClause.Occur.MUST);
    Optional<BooleanClause.Occur> mustNot =
        clauseOccurIfChildPresent.apply(mustNot(), BooleanClause.Occur.MUST_NOT);
    Optional<BooleanClause.Occur> should =
        clauseOccurIfChildPresent.apply(should(), BooleanClause.Occur.SHOULD);
    Optional<BooleanClause.Occur> filter =
        clauseOccurIfChildPresent.apply(filter(), BooleanClause.Occur.FILTER);

    List<Optional<BooleanClause.Occur>> occurWhereChildIsPresent =
        Stream.of(must, mustNot, should, filter)
            .filter(Optional::isPresent)
            .collect(Collectors.toList());

    checkState(
        occurWhereChildIsPresent.size() <= 1, "child should be present in zero or one clause set");

    return occurWhereChildIsPresent.size() == 1
        ? occurWhereChildIsPresent.get(0)
        : Optional.empty();
  }

  @Override
  public void removeChild(QueryVisitorQueryExecutionContextNode child) {
    Optional<BooleanClause.Occur> childOccur = occurFor(child);

    if (childOccur.isEmpty()) {
      return;
    }

    switch (childOccur.get()) {
      case MUST -> {
        must().remove(child);
      }
      case MUST_NOT -> {
        mustNot().remove(child);
      }
      case FILTER -> {
        filter().remove(child);
      }
      case SHOULD -> {
        should().remove(child);
      }
    }
  }

  RewrittenChildClauses rewritten() throws RewrittenQueryNodeException {
    return RewrittenChildClauses.create(this);
  }

  @Override
  public String toString() {
    return toString(
        this.mustChildren, this.mustNotChildren, this.shouldChildren, this.filterChildren);
  }
}
