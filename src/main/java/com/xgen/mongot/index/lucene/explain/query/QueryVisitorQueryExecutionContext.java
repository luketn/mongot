package com.xgen.mongot.index.lucene.explain.query;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.util.Check;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

public class QueryVisitorQueryExecutionContext
    implements QueryExecutionContext<QueryVisitorQueryExecutionContextNode> {

  private final ExplainVisitor visitor;
  private final IdentityHashMap<Query, QueryVisitorQueryExecutionContextNode> contextNodeMap;
  private Optional<QueryVisitorQueryExecutionContextNode> root;

  public QueryVisitorQueryExecutionContext() {
    this.contextNodeMap = new IdentityHashMap<>();
    this.visitor = new ExplainVisitor(this.contextNodeMap);
    this.root = Optional.empty();
  }

  @Override
  public Optional<QueryVisitorQueryExecutionContextNode> getOrCreateNode(Query query) {
    // If we've already called visit() on a parent of this query, get the context node that was
    // already created from the contextNodeMap.
    if (this.root.isPresent()) {
      return Optional.ofNullable(this.contextNodeMap.get(query));
    }

    // Call query.visit() only once - on the first query we see. No need to traverse the tree
    // multiple times.
    query.visit(this.visitor);
    var node = this.contextNodeMap.get(query);
    this.root = Optional.of(node);
    return this.root;
  }

  @Override
  public void replaceNode(Query original, Query rewritten) {
    if (this.root.isEmpty()) {
      // If original is rewritten before we visited it, just createNode with the rewritten query.
      getOrCreateNode(rewritten);
      return;
    }

    if (!this.contextNodeMap.containsKey(original)) {
      // We haven't seen this query yet; it was likely created in a bulk scorer. We can't replace it
      // because we don't know what query it originated from.
      return;
    }

    QueryVisitorQueryExecutionContextNode originalNode = this.contextNodeMap.get(original);

    checkState(originalNode.parent.isPresent(), "original must have parent to be replaced");
    QueryVisitorQueryExecutionContextNode parentNode = originalNode.parent.get();

    checkState(
        parentNode.children.isPresent(),
        "parent of this node must have children - otherwise, what are we going to replace?");
    ChildClauses childrenOfParentNode = parentNode.children.get();

    // Get the occur defining the relationship between this node and it's parent - it will also
    // define the relationship between the rewritten node and it's parent.
    Optional<BooleanClause.Occur> originalOccur = childrenOfParentNode.occurFor(originalNode);
    checkState(originalOccur.isPresent(), "parent does not know this node is one of it's children");

    // Remove original node and it's children from our map of context nodes.
    removeNodeFromMap(originalNode);

    // Remove original node from the children of the parent node.
    childrenOfParentNode.removeChild(originalNode);

    // Create a visitor configured to traverse the rewritten node's subtree.
    var subvisitorForRewritten =
        new ExplainVisitor(parentNode, originalOccur.get(), this.contextNodeMap);

    // Actually visit the rewritten query subtree.
    rewritten.visit(subvisitorForRewritten);
  }

  @Override
  public void addChildNode(Query parent, Query child, BooleanClause.Occur occur) {
    if (!this.contextNodeMap.containsKey(parent)) {
      // This should never happen. The parent query will be the root vector query, which should be
      // added to the tree as the first step in explain
      return;
    }

    Check.isPresent(this.root, "root");
    QueryVisitorQueryExecutionContextNode parentNode = this.contextNodeMap.get(parent);

    // Create a visitor configured to traverse the rewritten node's subtree.
    var subvisitorForChild = new ExplainVisitor(parentNode, occur, this.contextNodeMap);

    // Actually visit the rewritten query subtree.
    child.visit(subvisitorForChild);
  }

  @Override
  public RewrittenQueryExecutionContextNode rewritten() throws RewrittenQueryNodeException {
    Check.isPresent(this.root, "root");
    return this.root.get().rewritten();
  }

  private void removeNodeFromMap(QueryVisitorQueryExecutionContextNode node) {
    if (node.getChildren().isPresent()) {
      var children = node.getChildren().get();
      Stream.of(children.must(), children.mustNot(), children.filter(), children.should())
          .flatMap(Collection::stream)
          .forEach(this::removeNodeFromMap);
    }
    this.contextNodeMap.remove(node.getQuery());
  }

  @Override
  public Optional<QueryVisitorQueryExecutionContextNode> getRoot() {
    return this.root;
  }

  @Override
  public String toString() {
    return this.root.map(QueryVisitorQueryExecutionContextNode::toString).orElse("<empty>");
  }
}
