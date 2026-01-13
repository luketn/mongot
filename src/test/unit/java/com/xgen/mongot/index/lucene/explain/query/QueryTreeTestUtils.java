package com.xgen.mongot.index.lucene.explain.query;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.search.Query;
import org.junit.Assert;

public class QueryTreeTestUtils {
  static void test(QueryVisitorQueryExecutionContextNode node, QueryTreeNode expected) {
    test(node, expected, Optional.empty());
  }

  static void test(
      QueryVisitorQueryExecutionContextNode node,
      QueryTreeNode expected,
      Optional<QueryTreeNode> parent) {
    Assert.assertEquals("query should be equal", node.query, expected.query);
    if (parent.isPresent()) {
      Assert.assertTrue("node expected to have parent", node.parent.isPresent());
      Assert.assertEquals("should have same parent", node.parent.get().query, parent.get().query);
    }

    if (node.children.isEmpty()) {
      Assert.assertTrue("expected children missing", expected.childClauses.isEmpty());
      return;
    }
    Assert.assertTrue("expected no children", expected.childClauses.isPresent());

    var kids = node.children.get();
    var expectedKids = expected.childClauses.get();
    test(kids.must(), expectedKids.mustChildren, Optional.of(expected));
    test(kids.mustNot(), expectedKids.mustNotChildren, Optional.of(expected));
    test(kids.should(), expectedKids.shouldChildren, Optional.of(expected));
    test(kids.filter(), expectedKids.filterChildren, Optional.of(expected));
  }

  static void test(
      List<QueryVisitorQueryExecutionContextNode> clauses,
      List<QueryTreeNode> expectedClauses,
      Optional<QueryTreeNode> parent) {
    Assert.assertEquals(
        "size of clauses different from expected", clauses.size(), expectedClauses.size());

    // Sort for stable order of elements
    clauses.sort(Comparator.comparing(o -> o.query.toString()));
    expectedClauses.sort(Comparator.comparing(o -> o.query.toString()));
    for (int i = 0; i != clauses.size(); i++) {
      test(clauses.get(i), expectedClauses.get(i), parent);
    }
  }

  static class Children {
    final List<QueryTreeNode> mustChildren;
    final List<QueryTreeNode> mustNotChildren;
    final List<QueryTreeNode> shouldChildren;
    final List<QueryTreeNode> filterChildren;

    Children() {
      this.mustChildren = new ArrayList<>();
      this.mustNotChildren = new ArrayList<>();
      this.shouldChildren = new ArrayList<>();
      this.filterChildren = new ArrayList<>();
    }

    static Children create() {
      return new Children();
    }

    public Children must(Query... queries) {
      for (Query query : queries) {
        this.mustChildren.add(new QueryTreeNode(query, Optional.empty()));
      }
      return this;
    }

    public Children must(QueryTreeNode node) {
      this.mustChildren.add(node);
      return this;
    }

    public Children mustNot(Query... queries) {
      for (Query query : queries) {
        this.mustNotChildren.add(new QueryTreeNode(query, Optional.empty()));
      }
      return this;
    }

    public Children mustNot(QueryTreeNode node) {
      this.mustNotChildren.add(node);
      return this;
    }

    public Children should(Query... queries) {
      for (Query query : queries) {
        this.shouldChildren.add(new QueryTreeNode(query, Optional.empty()));
      }
      return this;
    }

    public Children should(QueryTreeNode node) {
      this.shouldChildren.add(node);
      return this;
    }

    public Children filter(Query... queries) {
      for (Query query : queries) {
        this.filterChildren.add(new QueryTreeNode(query, Optional.empty()));
      }
      return this;
    }

    public Children filter(QueryTreeNode node) {
      this.filterChildren.add(node);
      return this;
    }
  }

  static class QueryTreeNode {
    Query query;
    Optional<Children> childClauses;

    static class Builder {
      Query query;
      Optional<Children> children;

      Builder(Query query) {
        this.query = query;
        this.children = Optional.empty();
      }

      Builder children(Children children) {
        this.children = Optional.of(children);
        return this;
      }

      QueryTreeNode build() {
        return new QueryTreeNode(this.query, this.children);
      }
    }

    static Builder builder(Query query) {
      return new Builder(query);
    }

    QueryTreeNode(Query query, Optional<Children> children) {
      this.query = query;
      this.childClauses = children;
    }
  }
}
