package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;
import com.xgen.testing.mongot.index.query.scores.ScoreBuilder;
import java.util.Collections;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Assert;
import org.junit.Test;

public class TermLevelQueryFactoryTest {

  private static final String P_FIELD = "$type:string/p";
  private static final String KEYWORD_ANALYZER = StockAnalyzerNames.LUCENE_KEYWORD.getName();

  @Test
  public void testQueriesAreScored() throws Exception {
    var wildcard =
        OperatorBuilder.wildcard()
            .query("q")
            .path("p")
            .score(ScoreBuilder.valueBoost().value(3).build())
            .build();
    var expectedWildcard = new WildcardQuery(new Term(P_FIELD, "q"));
    var expectedWildcardQuery = new BoostQuery(expectedWildcard, 3);
    LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER)
        .assertTranslatedTo(wildcard, expectedWildcardQuery);

    var regex =
        OperatorBuilder.regex()
            .query("q")
            .path("p")
            .score(ScoreBuilder.valueBoost().value(3).build())
            .build();
    var expectedRegex = new RegexpQuery(new Term(P_FIELD, "q"));
    var expectedRegexQuery = new BoostQuery(expectedRegex, 3);
    LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER)
        .assertTranslatedTo(regex, expectedRegexQuery);
  }

  @Test
  public void testTextIsNormalizedButNotStemmed() throws Exception {
    var operator =
        OperatorBuilder.wildcard().allowAnalyzedField(true).query("Dogs").path("p").build();
    var expected = new WildcardQuery(new Term(P_FIELD, "dogs"));
    LuceneSearchTranslation.analyzer(StockAnalyzerNames.LUCENE_ENGLISH.getName())
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testTextIsNormalizedButNotTokenized() throws Exception {
    var operator =
        OperatorBuilder.regex().allowAnalyzedField(true).query("Dogs.* Cutes").path("p").build();
    var expected = new RegexpQuery(new Term(P_FIELD, "dogs.* cutes"));
    LuceneSearchTranslation.analyzer(StockAnalyzerNames.LUCENE_ENGLISH.getName())
        .assertTranslatedTo(operator, expected);
  }

  @Test
  public void testUsingAnalyzerWithoutAllowAnalyzedIsInvalid() {
    var operator = OperatorBuilder.regex().query("f").path("p").build();
    Assert.assertThrows(
        InvalidQueryException.class,
        () ->
            LuceneSearchTranslation.analyzer(StockAnalyzerNames.LUCENE_STANDARD.getName())
                .translate(operator));
  }

  @Test
  public void testAllowAnalyzedFieldsDoesntMatterForKeywordAnalyzer() throws Exception {
    LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER)
        .translate(OperatorBuilder.regex().query("f").path("p").build());

    LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER)
        .translate(OperatorBuilder.regex().allowAnalyzedField(true).query("f").path("p").build());
  }

  @Test
  public void testMultipleQueriesCreatesDisjunction() throws Exception {
    var operator =
        OperatorBuilder.wildcard()
            .allowAnalyzedField(true)
            .query("q1")
            .query("q2")
            .path("p")
            .build();
    var expected =
        BooleanComposer.should(
            new WildcardQuery(new Term(P_FIELD, "q1")), new WildcardQuery(new Term(P_FIELD, "q2")));
    LuceneSearchTranslation.get().assertTranslatedTo(operator, expected);
  }

  @Test
  public void testMultiplePathsCreatesDisjunction() throws Exception {
    var operator = OperatorBuilder.regex().query("q").path("p1").path("p2").build();
    var expected =
        BooleanComposer.should(
            new RegexpQuery(new Term(P_FIELD + "1", "q")),
            new RegexpQuery(new Term(P_FIELD + "2", "q")));
    LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER).assertTranslatedTo(operator, expected);
  }

  @Test
  public void testRegexResolvesWildcardPath() throws Exception {
    var operator =
        OperatorBuilder.regex()
            .query("q")
            .path(UnresolvedStringPathBuilder.wildcardPath("a.*"))
            .build();
    var expected = new RegexpQuery(new Term("$type:string/a.b.c", "q"));
    var actual =
        LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER)
            .translateWithIndexedFields(operator, Collections.singletonList("a.b.c"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testWildcardResolvesWildcardPath() throws Exception {
    var operator =
        OperatorBuilder.wildcard()
            .query("q")
            .path(UnresolvedStringPathBuilder.wildcardPath("a.*"))
            .build();
    var expected = new WildcardQuery(new Term("$type:string/a.b.c", "q"));
    var actual =
        LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER)
            .translateWithIndexedFields(operator, Collections.singletonList("a.b.c"));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testExistsQueriesAreScored() throws Exception {
    var operator =
        OperatorBuilder.exists()
            .score(ScoreBuilder.valueBoost().value(3).build())
            .path("q")
            .build();
    var expected =
        new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("$meta/fieldNames", "q"))), 3);
    LuceneSearchTranslation.analyzer(KEYWORD_ANALYZER).assertTranslatedTo(operator, expected);
  }
}
