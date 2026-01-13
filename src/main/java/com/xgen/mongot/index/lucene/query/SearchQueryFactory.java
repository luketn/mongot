package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.field.FieldName.getLuceneFieldNameForStringPath;
import static org.apache.lucene.index.MultiTerms.getTerms;

import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.util.AnalyzedText;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SearchPhraseOperator;
import com.xgen.mongot.index.query.operators.SearchPhrasePrefixOperator;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

class SearchQueryFactory {

  private static final int NO_SPLITERATOR_CHARACTERISTICS = 0;

  private final SearchQueryFactoryContext queryFactoryContext;
  private final AllDocsQueryFactory allDocsQueryFactory;

  SearchQueryFactory(
      SearchQueryFactoryContext queryFactoryContext, AllDocsQueryFactory allDocsQueryFactory) {
    this.queryFactoryContext = queryFactoryContext;
    this.allDocsQueryFactory = allDocsQueryFactory;
  }

  private Query fromSearchPhrase(
      SearchPhraseOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {

    return BooleanComposer.StreamUtils.from(
            StringPathQuery.product(operator.paths(), operator.query()))
        .mapOptionalChecked(
            pq ->
                this.queryFactoryContext
                    .safeQueryBuilder(pq.getPath(), singleQueryContext.getEmbeddedRoot())
                    .createPhraseQuery(
                        getLuceneFieldNameForStringPath(
                            pq.getPath(), singleQueryContext.getEmbeddedRoot()),
                        pq.getQuery(),
                        operator.slop()),
            BooleanClause.Occur.SHOULD);
  }

  /**
   * parses a Lucene QueryString. This method lives in SearchQueryFactory since it is potentially
   * analyzed.
   *
   * @param operator operator
   * @return Lucene Query
   */
  Query fromQueryString(QueryStringOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    // get defaultAnalyzer
    StringFieldPath stringFieldPath = new StringFieldPath(FieldPath.parse(operator.defaultPath()));
    return QueryStringParser.createQuery(
        this.allDocsQueryFactory,
        this.queryFactoryContext.getAnalyzer(stringFieldPath, Optional.empty()),
        singleQueryContext,
        operator.query(),
        operator.defaultPath(),
        singleQueryContext.getEmbeddedRoot());
  }

  Query fromSearch(SearchOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    // TODO(CLOUDP-280897): Support Dismax. The way to support dismax is to push the dismax query
    // down
    return switch (operator) {
      case SearchPhrasePrefixOperator searchPhrasePrefixOperator ->
          fromSearchPhrasePrefix(searchPhrasePrefixOperator, singleQueryContext);
      case SearchPhraseOperator searchPhraseOperator ->
          fromSearchPhrase(searchPhraseOperator, singleQueryContext);
      case SearchOperator searchOperator -> fromUnscoredSearch(searchOperator, singleQueryContext);
    };
  }

  private Query fromUnscoredSearch(SearchOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {

    return BooleanComposer.StreamUtils.from(
            StringPathQuery.resolvedProduct(operator.paths(), operator.query()))
        .mapOptionalChecked(
            pq ->
                this.queryFactoryContext
                    .safeQueryBuilder(pq.getPath(), singleQueryContext.getEmbeddedRoot())
                    .createBooleanQuery(
                        getLuceneFieldNameForStringPath(
                            pq.getPath(), singleQueryContext.getEmbeddedRoot()),
                        pq.getQuery()),
            BooleanClause.Occur.SHOULD);
  }

  private Query fromSearchPhrasePrefix(
      SearchPhrasePrefixOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return BooleanComposer.StreamUtils.from(
            StringPathQuery.product(operator.paths(), operator.query()))
        .mapChecked(
            pq ->
                createSearchPhrasePrefixQuery(
                    singleQueryContext.getIndexReader(),
                    operator.slop(),
                    operator.maxExpansions(),
                    pq.getPath(),
                    pq.getQuery(),
                    singleQueryContext.getEmbeddedRoot()),
            BooleanClause.Occur.SHOULD);
  }

  /**
   * Creates a search phrase prefix match, which can be a "good-enough" but not exhaustive match.
   * Builds a phrase query match, where the last element of the phrase is a disjunction of expanding
   * whatever it finds in the terms list. This algorithm may miss terms since ordering is based upon
   * how terms are ordered in the index, rather than any reference to the query.
   *
   * <p>For a query like "I love S", and the lucene.english analyzer, the query expands to
   * PhraseQuery(i, love, [Sauce, Songs, Saint, San, Santa, San etc]) up until the maxExpansions
   * number.
   *
   * @param indexReader indexReader needed to walk over terms list
   * @param slop determines slop in phrase match.
   * @param maxExpansions number of terms to expand last phrase to. The larger this number, the more
   *     resource intensive the query, but also the more recall the search has, and less likely to
   *     miss.
   * @param stringPath paths to search
   * @param queryText queries to search
   * @return phrase query to match
   */
  private Query createSearchPhrasePrefixQuery(
      IndexReader indexReader,
      int slop,
      int maxExpansions,
      StringPath stringPath,
      String queryText,
      Optional<FieldPath> embeddedRoot)
      throws InvalidQueryException {

    Analyzer analyzer = this.queryFactoryContext.getAnalyzer(stringPath, embeddedRoot);

    MultiPhraseQuery.Builder builder = new MultiPhraseQuery.Builder();
    builder.setSlop(slop);

    try {
      List<String> terms =
          AnalyzedText.applyAnalyzer(analyzer, stringPath, queryText, embeddedRoot);

      if (terms.isEmpty()) {
        return new MatchNoDocsQuery();
      }

      int size = terms.size();
      // get all the terms before the last, and add those to the builder
      terms.stream()
          .limit(size - 1)
          .map(token -> new Term(getLuceneFieldNameForStringPath(stringPath, embeddedRoot), token))
          .forEach(builder::add);

      // get the last term, and expand it up to "maxExpansions"
      String lastTerm = terms.get(size - 1);
      List<Term> suggestions =
          getTermsWithPrefix(
              indexReader,
              getLuceneFieldNameForStringPath(stringPath, embeddedRoot),
              lastTerm,
              maxExpansions);

      // need this to avoid Array out of bounds exception
      if (suggestions.isEmpty()) {
        return builder.build();
      }

      // add the expansions as a single disjunction to the builder
      builder.add(suggestions.toArray(new Term[0]));

      return builder.build();

    } catch (IOException ex) {
      // TODO(CLOUDP-280897): code this error and do not expose to user: CLOUDP-43265
      throw new InvalidQueryException(ex.toString());
    }
  }

  /**
   * Given a Lucene field and an indexReader, will return matching terms.
   *
   * @param indexReader index to query for terms
   * @param luceneField field to look for terms
   * @param prefix prefix to use
   * @param maxExpansions number of terms to expand the query to
   * @return List<Term></Term>
   * @throws IOException IOException
   */
  private List<Term> getTermsWithPrefix(
      IndexReader indexReader, String luceneField, String prefix, int maxExpansions)
      throws IOException {
    Check.argNotEmpty(luceneField, "luceneField");
    Check.argNotEmpty(prefix, "prefix");

    // retrieve terms matching this path
    Terms terms = getTerms(indexReader, luceneField);

    // may return null, means no terms found
    if (terms == null) {
      return Collections.emptyList();
    }

    // wrap our iterator in a Iterator<String>, so it can be consumed by a stream
    Spliterator<String> spliterator =
        Spliterators.spliteratorUnknownSize(
            new TermsEnumPrefixIterator(terms.iterator(), prefix), NO_SPLITERATOR_CHARACTERISTICS);

    // stream out and filter our terms, and throw any IOExceptions from TermsEnum
    try {
      // return up to maxExpansions terms matching the prefix
      return StreamSupport.stream(spliterator, false)
          .takeWhile(t -> t.startsWith(prefix))
          .limit(maxExpansions)
          .map(t -> new Term(luceneField, t))
          .collect(Collectors.toList());
    } catch (UncheckedIOException exception) {
      throw exception.getCause();
    }
  }

  private static class TermsEnumPrefixIterator implements Iterator<String> {

    private final TermsEnum termsEnum;
    private final String prefix;

    private Optional<String> next = Optional.empty();

    /**
     * Wraps a Lucene TermsEnum iterator into a generic String Iterator so it can be consumed in a
     * more idiomatic way.
     *
     * <p>Note: We immediately convert BytesRef to String because BytesRef is mutable and changes
     * when TermsEnum::next is called, which can cause surprising behaviors if you are not careful.
     * So, be cautious about passing around, inspecting or otherwise using BytesRef in a
     * non-localized fashion.
     *
     * <p>TODO(CLOUDP-280897): We may wish to factor out a plain TermsEnumIterator in the future,
     * without the prefix logic.
     *
     * @param termsEnum termsEnum
     * @param prefix prefix to filter terms to
     */
    TermsEnumPrefixIterator(TermsEnum termsEnum, String prefix) {
      this.termsEnum = termsEnum;
      this.prefix = prefix;
      this.init();
    }

    @Override
    public boolean hasNext() {
      return this.next.isPresent();
    }

    void init() {
      try {
        // seekCeil seeks to the next term after prefix, but if prefix is past the last term, it
        // will go to an invalid cursor position, so we shouldn't have a next.
        if (this.termsEnum.seekCeil(new BytesRef(this.prefix)) == TermsEnum.SeekStatus.END) {
          this.next = Optional.empty();
        } else {
          this.next = Optional.ofNullable(this.termsEnum.term().utf8ToString());
        }
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    @Override
    public String next() {

      try {
        Optional<String> current = this.next;
        this.next = Optional.ofNullable(this.termsEnum.next()).map(BytesRef::utf8ToString);
        return current.orElseThrow(NoSuchElementException::new);

      } catch (IOException ioException) {
        throw new UncheckedIOException(ioException);
      }
    }
  }
}
