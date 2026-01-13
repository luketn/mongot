package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.field.FieldName.getLuceneFieldNameForStringPath;

import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.TermFuzzyOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TermPrefixOperator;
import com.xgen.mongot.index.query.operators.TermRegexOperator;
import com.xgen.mongot.index.query.operators.TermWildcardOperator;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import java.util.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;

class TermQueryFactory {

  private final SearchQueryFactoryContext queryFactoryContext;

  TermQueryFactory(SearchQueryFactoryContext queryFactoryContext) {
    this.queryFactoryContext = queryFactoryContext;
  }

  Query fromTerm(TermOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    /*
     * TODO(CLOUDP-280897): These should be using TermInSetQuery if the paths are all the same since
     * it'll be faster then BooleanQuery.
     */
    try {
      return BooleanComposer.StreamUtils.from(
              StringPathQuery.resolvedProduct(operator.paths(), operator.query()))
          .mapChecked(getQueryBuilder(operator, singleQueryContext), BooleanClause.Occur.SHOULD);
    } catch (IllegalArgumentException e) {
      var backslashMisuse =
          Optional.ofNullable(e.getMessage())
              .map(message -> message.contains("invalid character class"))
              .orElse(false);
      if (backslashMisuse) {
        // TODO(CLOUDP-125626): remove this
        throw new InvalidQueryException(
            "Inappropriate use of backslashes is not allowed. Starting from Lucene 9,"
                + "an expression like '\\p' is not interpreted as the letter 'p' anymore."
                + "See LUCENE-9370 for details.",
            InvalidQueryException.Type.LENIENT);
      }
      throw e;
    }
  }

  private CheckedFunction<StringPathQuery, ? extends Query, InvalidQueryException> getQueryBuilder(
      TermOperator operator, SingleQueryContext singleQueryContext) {
    return switch (operator.getType()) {
      case TERM -> pq -> new TermQuery(normalizedTerm(pq, singleQueryContext));
      default -> getMultiTermQueryBuilder(operator, singleQueryContext);
    };
  }

  CheckedFunction<StringPathQuery, ? extends MultiTermQuery, InvalidQueryException>
      getMultiTermQueryBuilder(TermOperator operator, SingleQueryContext singleQueryContext) {
    return switch (operator) {
      case TermFuzzyOperator termFuzzyOperator ->
          pq ->
              new FuzzyQuery(
                  normalizedTerm(pq, singleQueryContext),
                  termFuzzyOperator.maxEdits(),
                  termFuzzyOperator.prefixLength(),
                  termFuzzyOperator.maxExpansions(),
                  FuzzyQuery.defaultTranspositions);
      case TermPrefixOperator termPrefixOperator ->
          pq -> new PrefixQuery(normalizedTerm(pq, singleQueryContext));
      case TermRegexOperator termRegexOperator ->
          pq -> new RegexpQuery(normalizedTerm(pq, singleQueryContext));
      case TermWildcardOperator termWildcardOperator ->
          pq -> new WildcardQuery(normalizedTerm(pq, singleQueryContext));
      case TermOperator termOperator -> throw new UnsupportedOperationException();
    };
  }

  Term normalizedTerm(StringPathQuery pq, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    String field =
        getLuceneFieldNameForStringPath(pq.getPath(), singleQueryContext.getEmbeddedRoot());
    BytesRef normalized =
        this.queryFactoryContext
            .getAnalyzer(pq.getPath(), singleQueryContext.getEmbeddedRoot())
            .normalize(field, pq.getQuery());
    return new Term(field, normalized);
  }
}
