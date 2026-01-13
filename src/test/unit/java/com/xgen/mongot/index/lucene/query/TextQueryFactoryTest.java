package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.operators.FuzzyOption;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.operators.TextOperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class TextQueryFactoryTest {
  private static final String synonymMappingName = "standard_synonyms";

  @Test
  public void testTextQueryIsScored() throws Exception {
    var operator =
        OperatorBuilder.text()
            .query("q")
            .path("p")
            .score(ScoreBuilder.valueBoost().value(3).build())
            .build();
    var inner = new TermQuery(new Term("$type:string/p", "q"));
    var expected = new BoostQuery(inner, 3);
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testTermTextIsAnalyzed() throws Exception {
    var operator = OperatorBuilder.text().query("Q ").path("p").build();
    var expected = new TermQuery(new Term("$type:string/p", "q"));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testFuzzyTermTextIsAnalyzed() throws Exception {
    var operator =
        OperatorBuilder.text()
            .fuzzy(
                TextOperatorBuilder.fuzzyBuilder()
                    .maxEdits(2)
                    .prefixLength(2)
                    .maxExpansions(2)
                    .build())
            .query("Q ")
            .path("p")
            .build();
    var expected = fuzzyQuery("$type:string/p", "q", 2, 2, 2);
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testJustStopwordsDoesNotMatchAnythingAndDoesNotCrash() throws Exception {
    var operator = OperatorBuilder.text().query("and the").path("p").build();
    var expected = new MatchNoDocsQuery("Query analysis produced no terms");
    var actual =
        LuceneSearchTranslation.analyzer(StockAnalyzerNames.LUCENE_ENGLISH.getName())
            .translate(operator);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testEmptyQueryMatchesNothing() throws Exception {
    var operator = OperatorBuilder.text().query("").path("p").build();
    var expected = new MatchNoDocsQuery("Query analysis produced no terms");
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testEmptyFuzzyQueryMatchesNothing() throws Exception {
    var operator =
        OperatorBuilder.text()
            .fuzzy(TextOperatorBuilder.fuzzyBuilder().build())
            .query("")
            .path("p")
            .build();
    var expected = new MatchNoDocsQuery("Query analysis produced no terms");
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultipleQueriesCreatesDisjunction() throws Exception {
    var operator = OperatorBuilder.text().query("q1").query("q2").path("p").build();

    var expected =
        BooleanComposer.should(
            new TermQuery(new Term("$type:string/p", "q1")),
            new TermQuery(new Term("$type:string/p", "q2")));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiQueryAndPathsCreatesDisjunctionFromAllCombinations() throws Exception {
    var operator = OperatorBuilder.text().query("q1").query("q2").path("p1").path("p2").build();
    var actualClauses =
        ((BooleanQuery) LuceneSearchTranslation.get().translate(operator)).clauses();

    var expectedClauses =
        Arrays.asList(
            BooleanComposer.shouldClause(new TermQuery(new Term("$type:string/p1", "q1"))),
            BooleanComposer.shouldClause(new TermQuery(new Term("$type:string/p1", "q2"))),
            BooleanComposer.shouldClause(new TermQuery(new Term("$type:string/p2", "q1"))),
            BooleanComposer.shouldClause(new TermQuery(new Term("$type:string/p2", "q2"))));
    Assert.assertEquals(4, actualClauses.size());
    //    Do not care about the order:
    Assert.assertTrue(expectedClauses.containsAll(actualClauses));
    Assert.assertTrue(actualClauses.containsAll(expectedClauses));
  }

  @Test
  public void testTrivialFuzzyExample() throws Exception {
    var operator =
        OperatorBuilder.text()
            .fuzzy(TextOperatorBuilder.fuzzyBuilder().maxEdits(1).maxExpansions(29).build())
            .query("q")
            .path("p")
            .build();
    var expected =
        fuzzyQuery(
            "$type:string/p", "q", 1, FuzzyOption.Fields.PREFIX_LENGTH.getDefaultValue(), 29);
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiTokenFuzzyCreatesAFuzzyQueryForEachToken() throws Exception {
    var operator =
        OperatorBuilder.text()
            .fuzzy(TextOperatorBuilder.fuzzyBuilder().maxExpansions(29).build())
            .query("q1 q2")
            .path("p")
            .build();
    var expectedMaxEdits = FuzzyOption.Fields.MAX_EDITS.getDefaultValue();
    var expectedPrefixLen = FuzzyOption.Fields.PREFIX_LENGTH.getDefaultValue();
    var expected =
        BooleanComposer.should(
            fuzzyQuery("$type:string/p", "q1", expectedMaxEdits, expectedPrefixLen, 29),
            fuzzyQuery("$type:string/p", "q2", expectedMaxEdits, expectedPrefixLen, 29));

    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiTokenFuzzyQueryForVariousPathsCreatesNestedDisjunctions() throws Exception {
    var operator =
        OperatorBuilder.text()
            .fuzzy(
                TextOperatorBuilder.fuzzyBuilder()
                    .maxEdits(2)
                    .maxExpansions(2)
                    .prefixLength(2)
                    .build())
            .query("q1 q2")
            .path("p1")
            .path("p2")
            .build();

    // One should clause per fuzzy token nested under a should clause per path
    var expected =
        BooleanComposer.should(
            BooleanComposer.should(
                fuzzyQuery("$type:string/p1", "q1", 2, 2, 2),
                fuzzyQuery("$type:string/p1", "q2", 2, 2, 2)),
            BooleanComposer.should(
                fuzzyQuery("$type:string/p2", "q1", 2, 2, 2),
                fuzzyQuery("$type:string/p2", "q2", 2, 2, 2)));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testTextResolvesWildcardPath() throws Exception {
    var operator =
        OperatorBuilder.text()
            .path(UnresolvedStringPathBuilder.wildcardPath("a.*"))
            .query("q")
            .build();
    var expected = new TermQuery(new Term("$type:string/a.b.c", "q"));
    var actual =
        LuceneSearchTranslation.get()
            .translateWithIndexedFields(operator, Collections.singletonList("a.b.c"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSingleWordSynonymsAny() throws Exception {
    var operator =
        OperatorBuilder.text()
            .query("fast car")
            .path("p")
            .matchCriteria(TextOperator.MatchCriteria.ANY)
            .synonyms(synonymMappingName)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick");

    var expected =
        BooleanComposer.should(
            BooleanComposer.should(
                new SynonymQuery.Builder("$type:string/p")
                    .addTerm(new Term("$type:string/p", "fast"))
                    .addTerm(new Term("$type:string/p", "quick"))
                    .addTerm(new Term("$type:string/p", "fast"))
                    .build(),
                new TermQuery(new Term("$type:string/p", "car"))),
            new BoostQuery(
                BooleanComposer.should(
                    new TermQuery(new Term("$type:string/p", "fast")),
                    new TermQuery(new Term("$type:string/p", "car"))),
                2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testSingleWordSynonymsAll() throws Exception {
    var operator =
        OperatorBuilder.text()
            .query("fast car")
            .path("p")
            .matchCriteria(TextOperator.MatchCriteria.ALL)
            .synonyms(synonymMappingName)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick");

    var expected =
        BooleanComposer.should(
            BooleanComposer.must(
                new SynonymQuery.Builder("$type:string/p")
                    .addTerm(new Term("$type:string/p", "fast"))
                    .addTerm(new Term("$type:string/p", "quick"))
                    .addTerm(new Term("$type:string/p", "fast"))
                    .build(),
                new TermQuery(new Term("$type:string/p", "car"))),
            new BoostQuery(
                BooleanComposer.must(
                    new TermQuery(new Term("$type:string/p", "fast")),
                    new TermQuery(new Term("$type:string/p", "car"))),
                2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiWordSynonymsAny() throws Exception {
    var operator =
        OperatorBuilder.text()
            .query("fast car")
            .path("p")
            .matchCriteria(TextOperator.MatchCriteria.ANY)
            .synonyms(synonymMappingName)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick", "with speed in mind");

    var expected =
        BooleanComposer.should(
            BooleanComposer.should(
                new DisjunctionMaxQuery(
                    List.of(
                        new TermQuery(new Term("$type:string/p", "fast")),
                        new TermQuery(new Term("$type:string/p", "fast")),
                        new TermQuery(new Term("$type:string/p", "quick")),
                        new PhraseQuery("$type:string/p", "with", "speed", "in", "mind")),
                    0.1f),
                new TermQuery(new Term("$type:string/p", "car"))),
            new BoostQuery(
                BooleanComposer.should(
                    new TermQuery(new Term("$type:string/p", "fast")),
                    new TermQuery(new Term("$type:string/p", "car"))),
                2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiWordSynonymsAll() throws Exception {
    var operator =
        OperatorBuilder.text()
            .query("fast car")
            .path("p")
            .matchCriteria(TextOperator.MatchCriteria.ALL)
            .synonyms(synonymMappingName)
            .build();
    List<String> equivalentSynonyms = List.of("fast", "quick", "with speed in mind");

    var expected =
        BooleanComposer.should(
            BooleanComposer.must(
                new DisjunctionMaxQuery(
                    List.of(
                        new TermQuery(new Term("$type:string/p", "fast")),
                        new TermQuery(new Term("$type:string/p", "fast")),
                        new TermQuery(new Term("$type:string/p", "quick")),
                        new PhraseQuery("$type:string/p", "with", "speed", "in", "mind")),
                    0.1f),
                new TermQuery(new Term("$type:string/p", "car"))),
            new BoostQuery(
                BooleanComposer.must(
                    new TermQuery(new Term("$type:string/p", "fast")),
                    new TermQuery(new Term("$type:string/p", "car"))),
                2));

    SynonymTestUtil.synonymsTest(equivalentSynonyms, synonymMappingName)
        .assertTranslatedTo(operator, expected);
  }

  private FuzzyQuery fuzzyQuery(
      String field, String termText, int maxEdits, int prefixLen, int maxExpansions) {
    return new FuzzyQuery(
        new Term(field, termText),
        maxEdits,
        prefixLen,
        maxExpansions,
        FuzzyQuery.defaultTranspositions);
  }
}
