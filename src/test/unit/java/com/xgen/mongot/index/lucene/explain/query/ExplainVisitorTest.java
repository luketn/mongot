package com.xgen.mongot.index.lucene.explain.query;

import static com.xgen.mongot.index.lucene.explain.query.QueryTreeTestUtils.test;

import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.junit.Test;

public class ExplainVisitorTest {

  @Test
  public void testVisitsSimpleQuery() {
    var query = termFor("a");
    var expected = QueryTreeTestUtils.QueryTreeNode.builder(query).build();

    doTest(query, expected);
  }

  @Test
  public void testSimpleNested() {
    var a = termFor("a");
    var b = termFor("b");
    var c = termFor("c");
    var d = termFor("d");

    var query =
        new BooleanQuery.Builder()
            .add(a, BooleanClause.Occur.MUST)
            .add(b, BooleanClause.Occur.MUST_NOT)
            .add(c, BooleanClause.Occur.SHOULD)
            .add(d, BooleanClause.Occur.FILTER)
            .build();

    var expected =
        QueryTreeTestUtils.QueryTreeNode.builder(query)
            .children(QueryTreeTestUtils.Children.create().must(a).mustNot(b).should(c).filter(d))
            .build();

    doTest(query, expected);
  }

  @Test
  public void testDeepNested() {
    var a = termFor("a");
    var b = termFor("b");
    var c = termFor("c");
    var d = termFor("d");

    var ab =
        new BooleanQuery.Builder()
            .add(a, BooleanClause.Occur.SHOULD)
            .add(b, BooleanClause.Occur.SHOULD)
            .build();

    var cd =
        new BooleanQuery.Builder()
            .add(c, BooleanClause.Occur.SHOULD)
            .add(d, BooleanClause.Occur.SHOULD)
            .build();

    var query =
        new BooleanQuery.Builder()
            .add(ab, BooleanClause.Occur.MUST)
            .add(cd, BooleanClause.Occur.MUST)
            .build();

    var expected =
        QueryTreeTestUtils.QueryTreeNode.builder(query)
            .children(
                QueryTreeTestUtils.Children.create()
                    .must(
                        QueryTreeTestUtils.QueryTreeNode.builder(ab)
                            .children(QueryTreeTestUtils.Children.create().should(a).should(b))
                            .build())
                    .must(
                        QueryTreeTestUtils.QueryTreeNode.builder(cd)
                            .children(QueryTreeTestUtils.Children.create().should(c).should(d))
                            .build()))
            .build();

    doTest(query, expected);
  }

  @Test
  public void testRegex() throws Exception {
    try (Directory directory = new ByteBuffersDirectory()) {
      var writer = new IndexWriter(directory, new IndexWriterConfig());
      var reader = DirectoryReader.open(writer);

      var original = new RegexpQuery(new Term("foo", "a*"));
      var query = original.rewrite(new IndexSearcher(reader));

      // Regexp rewrites itself into a MultiTermQuery with
      // a single child of the original RegexpQuery.
      var expected =
          QueryTreeTestUtils.QueryTreeNode.builder(query)
              .children(QueryTreeTestUtils.Children.create().filter(original))
              .build();

      doTest(query, expected);
    }
  }

  @Test
  public void testPhrase() {
    Query query = new PhraseQuery(2, "foo", "a", "b");

    var expected = QueryTreeTestUtils.QueryTreeNode.builder(query).build();

    doTest(query, expected);
  }

  @Test
  public void testDismax() {
    var a = termFor("a");
    var b = termFor("b");
    Query query = new DisjunctionMaxQuery(List.of(a, b), 0.5f);

    var expected =
        QueryTreeTestUtils.QueryTreeNode.builder(query)
            .children(QueryTreeTestUtils.Children.create().should(a).should(b))
            .build();

    doTest(query, expected);
  }

  @Test
  public void testBoost() {
    var a = termFor("a");
    Query query = new BoostQuery(a, 10f);

    var expected =
        QueryTreeTestUtils.QueryTreeNode.builder(query)
            .children(QueryTreeTestUtils.Children.create().must(a))
            .build();

    doTest(query, expected);
  }

  @Test
  public void testAutocomplete() {
    var query =
        new AutomatonQuery(
            new Term("$type:autocomplete/description", "résumé"),
            new LevenshteinAutomata("umé", true).toAutomaton(1, "rés"));

    var expected = new QueryTreeTestUtils.QueryTreeNode.Builder(query).build();

    doTest(query, expected);
  }

  @Test
  public void testNear() {
    var query = LongField.newDistanceFeatureQuery("$type:date/start", 1f, 1569104127037L, 42);

    var expected = QueryTreeTestUtils.QueryTreeNode.builder(query).build();

    doTest(query, expected);
  }

  @Test
  public void testRange() {
    var intSubQuery =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            "$type:int64/quantity", Long.MIN_VALUE, 1L);

    var doubleSubQuery =
        org.apache.lucene.document.LongPoint.newRangeQuery(
            "$type:double/quantity",
            LuceneDoubleConversionUtils.toLong(-1.0 * Double.MAX_VALUE),
            LuceneDoubleConversionUtils.toLong(1.0));

    var query =
        new BooleanQuery.Builder()
            .add(intSubQuery, BooleanClause.Occur.SHOULD)
            .add(doubleSubQuery, BooleanClause.Occur.SHOULD)
            .build();

    var expected =
        QueryTreeTestUtils.QueryTreeNode.builder(query)
            .children(QueryTreeTestUtils.Children.create().should(intSubQuery, doubleSubQuery))
            .build();

    doTest(query, expected);
  }

  @Test
  public void testWildcard() {
    var query = new WildcardQuery(new Term("$type:string/a.b.c", "q*"));

    var expected = QueryTreeTestUtils.QueryTreeNode.builder(query).build();

    doTest(query, expected);
  }

  private static void doTest(Query query, QueryTreeTestUtils.QueryTreeNode expected) {
    ExplainVisitor visitor = new ExplainVisitor(new IdentityHashMap<>());

    query.visit(visitor);
    var root = visitor.parent.get();

    test(root, expected);
  }

  static Query termFor(String term) {
    return new TermQuery(new Term("foo", term));
  }
}
