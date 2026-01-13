package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

public class QueryStringParser extends QueryParser {

  private static final String DISALLOWED_FIELD_NAME_PREFIX = "$";

  private final AllDocsQueryFactory allDocsQueryFactory;
  private final SingleQueryContext singleQueryContext;
  private final Optional<FieldPath> embeddedRoot;

  private QueryStringParser(
      AllDocsQueryFactory allDocsQueryFactory,
      SingleQueryContext singleQueryContext,
      Analyzer analyzer,
      String defaultField,
      Optional<FieldPath> embeddedRoot) {
    super(defaultField, analyzer);
    this.allDocsQueryFactory = allDocsQueryFactory;
    this.singleQueryContext = singleQueryContext;
    this.embeddedRoot = embeddedRoot;
    Check.argNotEmpty(defaultField, "defaultField");
  }

  /**
   * This class extends Lucene's classic QueryParser to create a Lucene Query from their query
   * syntax. We are extending it as it lets us validate illegal field names such as $meta: which
   * could be used to exfiltrate customer data with a maliciously formed query.
   *
   * @param allDocsFactory A factory that produces a lucene query that matches all root documents
   * @param defaultAnalyzer The Analyzer applied to all search terms. Since queryString can operate
   *     on multiple fields, this analyzer should delegate on a per-field basis.
   * @param singleQueryContext Needed to construct AllDocsQuery
   * @param queryString Lucene Classic QueryString formatted query
   * @param defaultField The field to use when not prefixed with a ':'. This should be the
   *     user-visible field name, not the internal Lucene field.
   * @param embeddedRoot If an embedded root is provided, it is applied to all fields that appear in
   *     the query string
   * @return Lucene Query
   */
  static Query createQuery(
      AllDocsQueryFactory allDocsFactory,
      Analyzer defaultAnalyzer,
      SingleQueryContext singleQueryContext,
      String queryString,
      String defaultField,
      Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {
    Check.argNotEmpty(queryString, "queryString");
    Check.argNotEmpty(defaultField, "defaultField");

    try {
      // Note that we do not want to pass in the converted lucene field for the default field, as it
      // will be converted in getFieldQuery.
      QueryStringParser parser =
          new QueryStringParser(
              allDocsFactory, singleQueryContext, defaultAnalyzer, defaultField, embeddedRoot);
      return parser.parse(queryString);
    } catch (ParseException ex) {
      throw new InvalidQueryException(ex.getMessage());
    }
  }

  @Override
  protected Query getRangeQuery(
      String field,
      String lowerBound,
      String upperBound,
      boolean startInclusive,
      boolean endInclusive)
      throws ParseException {
    Check.argNotNull(field, "field");

    String luceneField = getValidatedLuceneField(field);
    // Lucene optionally supports normalizing strings in RangeQueries that look like dates.
    // We index dates as longs though, so we call newRangeQuery directly to bypass date coercion
    return super.newRangeQuery(luceneField, lowerBound, upperBound, startInclusive, endInclusive);
  }

  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    Check.argNotNull(field, "field");
    Check.argNotNull(termStr, "termStr");
    if ("*".equals(field) && "*".equals(termStr)) {
      return this.allDocsQueryFactory.fromAllDocuments(this.singleQueryContext);
    }

    String luceneField = getValidatedLuceneField(field);
    return super.getWildcardQuery(luceneField, termStr);
  }

  @Override
  protected Query getRegexpQuery(String field, String termStr) throws ParseException {
    Check.argNotNull(field, "field");
    Check.argNotNull(termStr, "termStr");

    try {
      String luceneField = getValidatedLuceneField(field);
      return super.getRegexpQuery(luceneField, termStr);
    } catch (IllegalArgumentException ex) {
      throw new ParseException(String.format("'%s' is not a valid regular expression", termStr));
    }
  }

  @Override
  protected Query getPrefixQuery(String field, String termStr) throws ParseException {
    Check.argNotNull(field, "field");
    Check.argNotNull(termStr, "termStr");

    String luceneField = getValidatedLuceneField(field);
    return super.getPrefixQuery(luceneField, termStr);
  }

  @Override
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity)
      throws ParseException {
    Check.argNotNull(field, "field");
    Check.argNotNull(termStr, "termStr");

    String luceneField = getValidatedLuceneField(field);
    return super.getFuzzyQuery(luceneField, termStr, minSimilarity);
  }

  @Override
  protected Query getFieldQuery(String field, String queryText, boolean quoted)
      throws ParseException {
    Check.argNotNull(field, "field");
    Check.argNotNull(queryText, "queryText");

    String luceneField = getValidatedLuceneField(field);
    return super.getFieldQuery(luceneField, queryText, quoted);
  }

  /** Converts the user-provided field name into the internal Lucene field name. */
  private String getValidatedLuceneField(String userField) throws ParseException {
    if (userField.startsWith(DISALLOWED_FIELD_NAME_PREFIX)) {
      throw new ParseException(
          String.format("field %s cannot begin with %s", userField, DISALLOWED_FIELD_NAME_PREFIX));
    }

    FieldPath fieldPath = FieldPath.parse(userField);
    return FieldName.TypeField.STRING.getLuceneFieldName(fieldPath, this.embeddedRoot);
  }
}
