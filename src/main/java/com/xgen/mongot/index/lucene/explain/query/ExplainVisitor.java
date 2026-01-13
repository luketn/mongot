package com.xgen.mongot.index.lucene.explain.query;

import static com.xgen.mongot.util.Check.checkState;

import java.util.IdentityHashMap;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

class ExplainVisitor extends QueryVisitor {
  public Optional<QueryVisitorQueryExecutionContextNode> parent;
  private Optional<BooleanClause.Occur> occur;
  private final IdentityHashMap<Query, QueryVisitorQueryExecutionContextNode> contextNodeMap;

  ExplainVisitor(IdentityHashMap<Query, QueryVisitorQueryExecutionContextNode> contextNodeMap) {
    this.parent = Optional.empty();
    this.occur = Optional.empty();
    this.contextNodeMap = contextNodeMap;
  }

  ExplainVisitor(
      QueryVisitorQueryExecutionContextNode parentNode,
      BooleanClause.Occur occur,
      IdentityHashMap<Query, QueryVisitorQueryExecutionContextNode> contextNodeMap) {
    this.parent = Optional.of(parentNode);
    this.occur = Optional.of(occur);
    this.contextNodeMap = contextNodeMap;
  }

  /**
   * Called by leaf queries that match on specific terms. Also, apparently some queries will visit
   * themselves, get a subvisitor, then add themselves as a child ({@link
   * org.apache.lucene.search.PhraseQuery} is one). Don't add this query as a child if it is already
   * present as the parent.
   */
  @Override
  public void consumeTerms(Query query, Term... terms) {
    if (this.parent.isPresent() && this.parent.get().query == query) {
      return;
    }
    addNode(query);
  }

  /** Called by leaf queries that do not match on terms. */
  @Override
  public void visitLeaf(Query query) {
    addNode(query);
  }

  private QueryVisitorQueryExecutionContextNode addNode(Query query) {
    var node = createNode(query);
    this.contextNodeMap.put(query, node);
    return node;
  }

  private QueryVisitorQueryExecutionContextNode createNode(Query query) {
    if (this.parent.isEmpty()) {
      this.parent = Optional.of(new QueryVisitorQueryExecutionContextNode(query));
      return this.parent.get();
    }

    // If this.parent is present already, this new query is a child of that query. Children may only
    // be added to a parent with an associated BooleanClause.Occur through getSubvisitor. If this
    // is a child, visitorOccur must already be set (or we have no way of knowing what
    // relationship this child has to it's parent).
    checkState(
        this.occur.isPresent(),
        "clause occur must be set if adding a query to a node that already has a root");

    return this.parent.get().addChild(query, this.occur.get());
  }

  /** Pulls a visitor instance for visiting child clauses of a query. */
  @Override
  public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
    // If this.parent is not set, this is the first query that this visitor has seen. Call addNode
    // to create a node for the parent query for this visitor, before creating a visitor for it's
    // children.
    if (this.parent.isEmpty()) {
      addNode(parent);
    }

    var parentNode = this.parent.get();

    // Use == intentionally here - we want to test that these are the same object.
    if (parentNode.query == parent) {
      // The parent node for this subvisitor is the same as the parent for this visitor; configure
      // this subvisitor to add subsequent queries with the specified occur. This can happen, for
      // example, in a BooleanQuery when getting a "should"-occur type subvisitor after using a
      // "must"-occur type subvisitor. See BooleanQuery.visit() to understand why we need to do this
      // more fully.
      this.occur = Optional.of(occur);
      return this;
    }

    // Parent is a new query; add it as a subquery with previously configured occur. Return a new
    // visitor for the new interior query node.
    var subtreeRoot = addNode(parent);
    return new ExplainVisitor(subtreeRoot, occur, this.contextNodeMap);
  }
}
