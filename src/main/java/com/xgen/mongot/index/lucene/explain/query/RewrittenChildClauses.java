package com.xgen.mongot.index.lucene.explain.query;

import static com.xgen.mongot.index.lucene.explain.query.RewrittenQueryExecutionContextNode.mergeTrees;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.NotImplementedException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.search.BooleanClause;

public class RewrittenChildClauses implements QueryChildren<RewrittenQueryExecutionContextNode> {
  private final Set<RewrittenQueryExecutionContextNode> mustChildren;
  private final Set<RewrittenQueryExecutionContextNode> mustNotChildren;
  private final Set<RewrittenQueryExecutionContextNode> shouldChildren;
  private final Set<RewrittenQueryExecutionContextNode> filterChildren;

  @VisibleForTesting
  public RewrittenChildClauses(
      Set<RewrittenQueryExecutionContextNode> mustChildren,
      Set<RewrittenQueryExecutionContextNode> mustNotChildren,
      Set<RewrittenQueryExecutionContextNode> shouldChildren,
      Set<RewrittenQueryExecutionContextNode> filterChildren) {
    this.mustChildren = mustChildren;
    this.mustNotChildren = mustNotChildren;
    this.shouldChildren = shouldChildren;
    this.filterChildren = filterChildren;
  }

  static RewrittenChildClauses create(ChildClauses childClauses)
      throws RewrittenQueryNodeException {
    Set<RewrittenQueryExecutionContextNode> must =
        rewriteAndDeduplicateIdenticalQueries(childClauses.must());

    Set<RewrittenQueryExecutionContextNode> mustNot =
        rewriteAndDeduplicateIdenticalQueries(childClauses.mustNot());

    Set<RewrittenQueryExecutionContextNode> should =
        rewriteAndDeduplicateIdenticalQueries(childClauses.should());

    Set<RewrittenQueryExecutionContextNode> filter =
        rewriteAndDeduplicateIdenticalQueries(childClauses.filter());

    return new RewrittenChildClauses(must, mustNot, should, filter);
  }

  @VisibleForTesting
  static Set<RewrittenQueryExecutionContextNode> rewriteAndDeduplicateIdenticalQueries(
      List<QueryVisitorQueryExecutionContextNode> nodes) throws RewrittenQueryNodeException {
    List<RewrittenQueryExecutionContextNode> rewrittenQueryExecutionContextNodes =
        CheckedStream.from(nodes.stream())
            .mapAndCollectChecked(QueryVisitorQueryExecutionContextNode::rewritten);

    Map<RewrittenQueryExecutionContextNode, RewrittenQueryExecutionContextNode> merged =
        CheckedStream.from(rewrittenQueryExecutionContextNodes.stream())
            .collectToMapChecked(
                node -> node, node -> node, RewrittenQueryExecutionContextNode::mergeTrees);

    return Set.copyOf(merged.values());
  }

  @VisibleForTesting
  static Optional<RewrittenChildClauses> mergeClauses(
      Optional<RewrittenChildClauses> first, Optional<RewrittenChildClauses> second)
      throws RewrittenQueryNodeException {
    long presentClauses = Stream.of(first, second).filter(Optional::isPresent).count();
    if (presentClauses == 0) {
      return Optional.empty();
    } else if (presentClauses == 1) {
      throw new RewrittenQueryNodeException(
          RewrittenQueryNodeException.messageWithClauses(
              "Cannot merge only one RewrittenChildClauses, both must be present", first, second));
    }

    Set<RewrittenQueryExecutionContextNode> mustChildren = new HashSet<>();
    Set<RewrittenQueryExecutionContextNode> mustNotChildren = new HashSet<>();
    Set<RewrittenQueryExecutionContextNode> shouldChildren = new HashSet<>();
    Set<RewrittenQueryExecutionContextNode> filterChildren = new HashSet<>();

    for (RewrittenQueryExecutionContextNode child : first.get().must()) {
      RewrittenQueryExecutionContextNode correspondingChild =
          findCorrespondingChild(child, second.get().must());
      mustChildren.add(mergeTrees(child, correspondingChild));
    }

    for (RewrittenQueryExecutionContextNode child : first.get().mustNot()) {
      RewrittenQueryExecutionContextNode correspondingChild =
          findCorrespondingChild(child, second.get().mustNot());
      mustNotChildren.add(mergeTrees(child, correspondingChild));
    }

    for (RewrittenQueryExecutionContextNode child : first.get().should()) {
      RewrittenQueryExecutionContextNode correspondingChild =
          findCorrespondingChild(child, second.get().should());
      shouldChildren.add(mergeTrees(child, correspondingChild));
    }

    for (RewrittenQueryExecutionContextNode child : first.get().filter()) {
      RewrittenQueryExecutionContextNode correspondingChild =
          findCorrespondingChild(child, second.get().filter());
      filterChildren.add(mergeTrees(child, correspondingChild));
    }

    return Optional.of(
        new RewrittenChildClauses(mustChildren, mustNotChildren, shouldChildren, filterChildren));
  }

  @VisibleForTesting
  static RewrittenQueryExecutionContextNode findCorrespondingChild(
      RewrittenQueryExecutionContextNode child,
      Set<RewrittenQueryExecutionContextNode> correspondingChildren)
      throws RewrittenQueryNodeException {
    List<RewrittenQueryExecutionContextNode> matchingNodes =
        correspondingChildren.stream().filter(c -> c.equals(child)).toList();

    if (matchingNodes.size() != 1) {
      throw new RewrittenQueryNodeException(
          RewrittenQueryNodeException.messageWithNodes(
              "Corresponding node not found in the clause set of the other tree",
              Optional.of(child),
              Optional.empty()));
    }

    return matchingNodes.getFirst();
  }

  @Override
  public Set<RewrittenQueryExecutionContextNode> must() {
    return this.mustChildren;
  }

  @Override
  public Set<RewrittenQueryExecutionContextNode> mustNot() {
    return this.mustNotChildren;
  }

  @Override
  public Set<RewrittenQueryExecutionContextNode> should() {
    return this.shouldChildren;
  }

  @Override
  public Set<RewrittenQueryExecutionContextNode> filter() {
    return this.filterChildren;
  }

  @Override
  public void addClause(RewrittenQueryExecutionContextNode child, BooleanClause.Occur occur) {
    // TODO(CLOUDP-281657): implement this
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public Optional<BooleanClause.Occur> occurFor(RewrittenQueryExecutionContextNode child) {
    // TODO(CLOUDP-281657): implement this
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public void removeChild(RewrittenQueryExecutionContextNode child) {
    // TODO(CLOUDP-281657): implement this
    throw new NotImplementedException("Not implemented");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RewrittenChildClauses that)) {
      return false;
    }
    return Objects.equals(this.mustChildren, that.mustChildren)
        && Objects.equals(this.mustNotChildren, that.mustNotChildren)
        && Objects.equals(this.shouldChildren, that.shouldChildren)
        && Objects.equals(this.filterChildren, that.filterChildren);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.mustChildren, this.mustNotChildren, this.shouldChildren, this.filterChildren);
  }

  @Override
  public String toString() {
    return toString(
        this.mustChildren, this.mustNotChildren, this.shouldChildren, this.filterChildren);
  }
}
