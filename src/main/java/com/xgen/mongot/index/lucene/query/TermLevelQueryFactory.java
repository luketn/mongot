package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.field.FieldName.getLuceneFieldNameForStringPath;
import static com.xgen.mongot.index.query.InvalidQueryException.Type;
import static com.xgen.mongot.index.query.InvalidQueryException.validate;

import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.RegexOperator;
import com.xgen.mongot.index.query.operators.TermLevelOperator;
import com.xgen.mongot.index.query.operators.WildcardOperator;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;

class TermLevelQueryFactory {

  private final SearchQueryFactoryContext queryFactoryContext;

  TermLevelQueryFactory(SearchQueryFactoryContext queryFactoryContext) {
    this.queryFactoryContext = queryFactoryContext;
  }

  Query fromWildcard(WildcardOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return forQueryType(operator, singleQueryContext, WildcardQuery::new);
  }

  Query fromRegex(RegexOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    try {
      return forQueryType(operator, singleQueryContext, RegexpQuery::new);
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
            Type.LENIENT);
      }
      throw new InvalidQueryException(e.getMessage());
    }
  }

  private Query forQueryType(
      TermLevelOperator operator,
      SingleQueryContext singleQueryContext,
      Function<Term, Query> luceneConstructor)
      throws InvalidQueryException {
    Query unscored =
        BooleanComposer.StreamUtils.from(
                StringPathQuery.resolveAndProduct(
                    singleQueryContext.getIndexReader(),
                    singleQueryContext.getEmbeddedRoot(),
                    operator.paths(),
                    operator.query()))
            .mapChecked(
                pq ->
                    luceneConstructor.apply(
                        normalizedTerm(pq, operator, singleQueryContext.getEmbeddedRoot())),
                BooleanClause.Occur.SHOULD);
    return unscored;
  }

  private Term normalizedTerm(
      StringPathQuery pq, TermLevelOperator operator, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    StringPath path = pq.getPath();
    Analyzer analyzer = validAnalyzer(operator, path, embeddedRoot);
    String fieldName = getLuceneFieldNameForStringPath(path, embeddedRoot);
    BytesRef normalized = analyzer.normalize(fieldName, pq.getQuery());
    return new Term(fieldName, normalized);
  }

  private Analyzer validAnalyzer(
      TermLevelOperator operator, StringPath path, Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    AnalyzerMeta analyzerMeta = this.queryFactoryContext.getAnalyzerMeta(path, embeddedRoot);
    // Since the queries here are not fully analyzed.
    // We avoid query-index analysis mismatch by requiring users to specify allowAnalyzedField: true
    if (!analyzerMeta.derivedFromKeyword()) {
      validate(
          operator.allowAnalyzedField(),
          "Field %s is analyzed. "
              + "Use a keyword analyzed field or set allowAnalyzedField to true.",
          path);
    }
    return analyzerMeta.getAnalyzer();
  }
}
