package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class PhraseQueryFactoryTest {

  private static final String synonymMappingName = "standard_synonyms";

  @Test
  public void testQueryIsScored() throws Exception {
    var operator =
        OperatorBuilder.phrase()
            .query("q1 q2")
            .path("p")
            .score(ScoreBuilder.valueBoost().value(3).build())
            .build();
    var inner = new PhraseQuery("$type:string/p", "q1", "q2");
    var expected = new BoostQuery(inner, 3);
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test(expected = InvalidQueryException.class)
  public void testDismaxCanNotBeApplied() throws Exception {
    var operator =
        OperatorBuilder.phrase()
            .query("q1 q2")
            .path("p")
            .score(ScoreBuilder.dismax().tieBreakerScore(3).build())
            .build();
    LuceneSearchTranslation.get().translate(operator);
  }

  @Test
  public void testOneTermDegeneratesIntoTermQuery() throws Exception {
    // This is done by lucene's (QueryBuilder), not us
    var operator = OperatorBuilder.phrase().query("q").path("p").build();
    var expected = new TermQuery(new Term("$type:string/p", "q"));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testOneTermDegeneratesIntoTermQueryWhenSlopIsPresent() throws Exception {
    var operatorWithSlop = OperatorBuilder.phrase().slop(3).query("q").path("p").build();
    var expectedWithSlop = new TermQuery(new Term("$type:string/p", "q"));
    LuceneSearchTranslation.get().assertTranslatedTo(operatorWithSlop, expectedWithSlop);
  }

  @Test
  public void testTextIsAnalyzed() throws Exception {
    var operator = OperatorBuilder.phrase().query("Q1 Q2    ").path("p").build();
    var expected = new PhraseQuery("$type:string/p", "q1", "q2");
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testEmptyTextDoesNotCrash() throws Exception {
    var operator = OperatorBuilder.phrase().query("").path("p").build();
    var expected = BooleanComposer.should();
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testJustStopwordsMatchesNothingAndDoesNotCrash() throws Exception {
    var operator = OperatorBuilder.phrase().query("the").path("p").build();
    var expected = BooleanComposer.should();
    LuceneSearchTranslation.analyzer(StockAnalyzerNames.LUCENE_ENGLISH.getName())
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testNonDefaultSlop() throws Exception {
    var operator = OperatorBuilder.phrase().slop(3).query("Q1 Q2    ").path("p").build();
    var expected = new PhraseQuery(3, "$type:string/p", "q1", "q2");
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultipleQueriesCreatesDisjunction() throws Exception {
    var operator = OperatorBuilder.phrase().query("a1 a2").query("b1 b2").path("p").build();
    var expected =
        BooleanComposer.should(
            new PhraseQuery("$type:string/p", "a1", "a2"),
            new PhraseQuery("$type:string/p", "b1", "b2"));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiplePathsCreatesDisjunction() throws Exception {
    var operator = OperatorBuilder.phrase().query("q1 q2").path("p1").path("p2").build();
    var expected =
        BooleanComposer.should(
            new PhraseQuery("$type:string/p1", "q1", "q2"),
            new PhraseQuery("$type:string/p2", "q1", "q2"));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testLuceneDoesNotParseQueryText() throws Exception {
    var operator = OperatorBuilder.phrase().query("+q1 \"a b\" ").path("p").build();
    var expected = new PhraseQuery("$type:string/p", "q1", "a", "b");
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testWildcardPathResolves() throws Exception {
    var operator =
        OperatorBuilder.phrase()
            .query("+q1 \"a b\" ")
            .path(UnresolvedStringPathBuilder.wildcardPath("a.*"))
            .build();
    var expected = new PhraseQuery("$type:string/a.b", "q1", "a", "b");
    var actual =
        LuceneSearchTranslation.get()
            .translateWithIndexedFields(operator, Collections.singletonList("a.b"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSingleWordSynonyms() throws Exception {
    var operator =
        OperatorBuilder.phrase()
            .path("fieldWithAnalyzer")
            .query("fast car")
            .synonyms(synonymMappingName)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick");

    MultiPhraseQuery.Builder multiPhraseQueryBuilder = new MultiPhraseQuery.Builder();
    multiPhraseQueryBuilder.add(
        new Term[] {
          new Term("$type:string/fieldWithAnalyzer", "fast"),
          new Term("$type:string/fieldWithAnalyzer", "quick"),
          new Term("$type:string/fieldWithAnalyzer", "fast"),
        });
    multiPhraseQueryBuilder.add(new Term("$type:string/fieldWithAnalyzer", "car"));

    var expected =
        BooleanComposer.should(
            multiPhraseQueryBuilder.build(),
            new BoostQuery(new PhraseQuery("$type:string/fieldWithAnalyzer", "fast", "car"), 2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testSingleWordSynonymsWithSlop() throws Exception {
    var operator =
        OperatorBuilder.phrase()
            .path("fieldWithAnalyzer")
            .query("fast car")
            .synonyms(synonymMappingName)
            .slop(2)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick");

    MultiPhraseQuery.Builder multiPhraseQueryBuilder = new MultiPhraseQuery.Builder();
    multiPhraseQueryBuilder.setSlop(2);
    multiPhraseQueryBuilder.add(
        new Term[] {
          new Term("$type:string/fieldWithAnalyzer", "fast"),
          new Term("$type:string/fieldWithAnalyzer", "quick"),
          new Term("$type:string/fieldWithAnalyzer", "fast"),
        });
    multiPhraseQueryBuilder.add(new Term("$type:string/fieldWithAnalyzer", "car"));

    var expected =
        BooleanComposer.should(
            multiPhraseQueryBuilder.build(),
            new BoostQuery(new PhraseQuery(2, "$type:string/fieldWithAnalyzer", "fast", "car"), 2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiWordSynonyms() throws Exception {
    var operator =
        OperatorBuilder.phrase()
            .path("fieldWithAnalyzer")
            .query("fast car")
            .synonyms(synonymMappingName)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick", "with haste");

    var expected =
        BooleanComposer.should(
            BooleanComposer.should(
                new PhraseQuery("$type:string/fieldWithAnalyzer", "fast", "car"),
                new PhraseQuery("$type:string/fieldWithAnalyzer", "quick", "car"),
                new PhraseQuery("$type:string/fieldWithAnalyzer", "with", "haste", "car"),
                new PhraseQuery("$type:string/fieldWithAnalyzer", "fast", "car")),
            new BoostQuery(new PhraseQuery("$type:string/fieldWithAnalyzer", "fast", "car"), 2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiWordSynonymsWithSlop() throws Exception {
    var operator =
        OperatorBuilder.phrase()
            .path("fieldWithAnalyzer")
            .query("fast car")
            .synonyms(synonymMappingName)
            .slop(2)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick", "with haste");

    var expected =
        BooleanComposer.should(
            BooleanComposer.should(
                new PhraseQuery(2, "$type:string/fieldWithAnalyzer", "fast", "car"),
                new PhraseQuery(2, "$type:string/fieldWithAnalyzer", "quick", "car"),
                new PhraseQuery(2, "$type:string/fieldWithAnalyzer", "with", "haste", "car"),
                new PhraseQuery(2, "$type:string/fieldWithAnalyzer", "fast", "car")),
            new BoostQuery(new PhraseQuery(2, "$type:string/fieldWithAnalyzer", "fast", "car"), 2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }
}
