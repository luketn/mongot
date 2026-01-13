package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.query.util.SafeQueryBuilder;
import com.xgen.mongot.index.lucene.util.AnalyzedText;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.FuzzyOption;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

class AutocompleteQueryFactory {
  // Setting this to true allows the levenshtein automata to count transpositions as an edit, which
  // is desired.
  private static final boolean FUZZY_WITH_TRANSPOSITIONS = true;

  private final SearchQueryFactoryContext context;

  AutocompleteQueryFactory(SearchQueryFactoryContext context) {
    this.context = context;
  }

  /**
   * Create an autocomplete query, boosting exact matches if a field is also indexed as a string.
   *
   * <p>Accomplish this by creating a boolean query with this logic:
   *
   * <ul>
   *   <li>must: (should: match any query clause to indexed autocomplete fragment,
   *       minimumShouldMatch: 1)
   *   <li>should: match any query clause to indexed string token, minimumShouldMatch: 0
   * </ul>
   */
  Query fromCompletion(AutocompleteOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return new BooleanQuery.Builder()
        .add(autocompleteDisjunction(operator, singleQueryContext), BooleanClause.Occur.MUST)
        .add(exactMatchBoostDisjunction(operator, singleQueryContext), BooleanClause.Occur.SHOULD)
        .setMinimumNumberShouldMatch(0)
        .build();
  }

  /** Create a boolean query with should clauses matching autocomplete-indexed fragments. */
  private Query autocompleteDisjunction(
      AutocompleteOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    FieldPath fieldPath = operator.path();

    Optional<AutocompleteFieldDefinition> autocompleteFieldDefinition =
        this.context.getAutocompleteDefinition(fieldPath, singleQueryContext.getEmbeddedRoot());

    InvalidQueryException.validate(
        autocompleteFieldDefinition.isPresent(),
        "autocomplete index field definition not present at path %s",
        fieldPath);
    //noinspection OptionalGetWithoutIsPresent - this is ok because of above QPE.validate call.
    AutocompleteFieldDefinition autocompleteField = autocompleteFieldDefinition.get();

    Optional<FuzzyOption> fuzzyOption = operator.fuzzy();
    if (fuzzyOption.isPresent()) {
      if (autocompleteField.getMaxGrams() < fuzzyOption.get().prefixLength()) {
        throw new InvalidQueryException(
            "minimum fuzzy prefix length must be less than the maximum indexed token length");
      }
    }

    return BooleanComposer.StreamUtils.from(
            queryTokensOf(
                operator,
                singleQueryContext,
                this.context.getAutocompleteBaseAnalyzer(autocompleteField),
                this.context.getAutocompleteIndexAnalyzer(autocompleteField)))
        .map(
            queryFactoryFor(
                FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(
                    fieldPath, singleQueryContext.getEmbeddedRoot()),
                fuzzyOption),
            BooleanClause.Occur.SHOULD);
  }

  Function<BytesRef, Query> queryFactoryFor(
      String luceneFieldPath, Optional<FuzzyOption> fuzzyOption) {
    if (fuzzyOption.isEmpty()) {
      return token -> queryFor(luceneFieldPath, token);
    }

    return token -> fuzzyQueryFor(luceneFieldPath, token, fuzzyOption.get());
  }

  /**
   * Gets a list of analyzed tokens from this query. First tokenizes using the base analyzer for
   * this autocomplete field, then normalizes with the analyzer used at index-time to apply
   * transformations like diacritic folding.
   */
  private static List<BytesRef> queryTokensOf(
      AutocompleteOperator operator,
      SingleQueryContext singleQueryContext,
      Analyzer baseAnalyzer,
      Analyzer indexAnalyzer)
      throws InvalidQueryException {
    String luceneFieldPath =
        FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(
            operator.path(), singleQueryContext.getEmbeddedRoot());

    // "Normalizing" a query string performs diacritic and case folding if they are configured in
    // the analyzer, and may also truncate the query string. Text is analyzed by the user-specified
    // "baseAnalyzer" first, then are normalized by the internal autocomplete "indexAnalyzer" using
    // this function.
    Function<String, BytesRef> normalize = token -> indexAnalyzer.normalize(luceneFieldPath, token);

    return switch (operator.tokenOrder()) {
      case ANY:
        try {
          yield ListUtils.union(
                  AnalyzedText.applyAnalyzer(
                      baseAnalyzer,
                      new StringFieldPath(operator.path()),
                      operator.query(),
                      singleQueryContext.getEmbeddedRoot()),
                  operator.query())
              .stream()
              .map(normalize)
              .distinct()
              .collect(Collectors.toList());
        } catch (IOException e) {
          throw new InvalidQueryException("error expanding query to order-agnostic form");
        }

      case SEQUENTIAL:
        yield operator.query().stream().map(normalize).distinct().collect(Collectors.toList());
    };
  }

  /**
   * Generate a simple query for completion. The "heavy lifting" for this query is done at
   * index-time, which lets this method generate fast queries for exact terms.
   */
  private static Query queryFor(String luceneFieldPath, BytesRef analyzedQueryToken) {
    return new TermQuery(new Term(luceneFieldPath, analyzedQueryToken));
  }

  private static Query fuzzyQueryFor(
      String luceneFieldPath, BytesRef queryBytes, FuzzyOption fuzzyOption) {
    String normalizedQueryString = queryBytes.utf8ToString();

    // If queryString is less than or equal to the minimum fuzzy prefix length, this query must be
    // an exact match.
    if (normalizedQueryString.length() <= fuzzyOption.prefixLength()) {
      return queryFor(luceneFieldPath, queryBytes);
    }

    String exactMatchPrefix = normalizedQueryString.substring(0, fuzzyOption.prefixLength());
    String suffix = normalizedQueryString.substring(fuzzyOption.prefixLength());

    LevenshteinAutomata distanceAutomaton =
        new LevenshteinAutomata(suffix, FUZZY_WITH_TRANSPOSITIONS);
    return new AutomatonQuery(
        new Term(luceneFieldPath, normalizedQueryString),
        distanceAutomaton.toAutomaton(fuzzyOption.maxEdits(), exactMatchPrefix));
  }

  /** Create a boolean query with should clauses matching text-indexed tokens. */
  private Query exactMatchBoostDisjunction(
      AutocompleteOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    StringFieldPath stringPath = new StringFieldPath(operator.path());
    return BooleanComposer.StreamUtils.from(operator.query())
        .mapOptionalChecked(
            token ->
                exactMatchClauseFor(operator.tokenOrder(), stringPath, singleQueryContext)
                    .apply(
                        this.context.safeQueryBuilder(
                            stringPath, singleQueryContext.getEmbeddedRoot()),
                        token),
            BooleanClause.Occur.SHOULD);
  }

  private BiFunction<SafeQueryBuilder, String, Optional<Query>> exactMatchClauseFor(
      AutocompleteOperator.TokenOrder tokenOrder,
      StringFieldPath stringPath,
      SingleQueryContext singleQueryContext) {
    String luceneFieldName =
        FieldName.getLuceneFieldNameForStringPath(stringPath, singleQueryContext.getEmbeddedRoot());
    return switch (tokenOrder) {
      case ANY -> (queryBuilder, token) -> queryBuilder.createBooleanQuery(luceneFieldName, token);
      case SEQUENTIAL ->
          (queryBuilder, token) -> queryBuilder.createPhraseQuery(luceneFieldName, token, 0);
    };
  }
}
