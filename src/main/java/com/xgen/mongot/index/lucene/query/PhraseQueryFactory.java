package com.xgen.mongot.index.lucene.query;

import static com.xgen.mongot.index.lucene.field.FieldName.getLuceneFieldNameForStringPath;

import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.lucene.query.util.SafeQueryBuilder;
import com.xgen.mongot.index.lucene.query.util.SynonymQueryUtil;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

class PhraseQueryFactory {

  private static final int SYNONYMS_EXACT_MATCH_ADDITIONAL_BOOST_VALUE = 2;
  private final SearchQueryFactoryContext queryFactoryContext;

  PhraseQueryFactory(SearchQueryFactoryContext queryFactoryContext) {
    this.queryFactoryContext = queryFactoryContext;
  }

  Query fromPhrase(PhraseOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return unscored(operator, singleQueryContext);
  }

  private Query unscored(PhraseOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    CheckedFunction<StringPathQuery, Optional<? extends Query>, InvalidQueryException> buildSingle;
    if (operator.synonyms().isPresent()) {
      buildSingle =
          pq ->
              singleSynonymPhraseQuery(
                  pq,
                  singleQueryContext.getEmbeddedRoot(),
                  operator.slop(),
                  operator.synonyms().get());
    } else {
      buildSingle =
          pq -> singlePhraseQuery(pq, singleQueryContext.getEmbeddedRoot(), operator.slop());
    }
    return BooleanComposer.StreamUtils.from(
            StringPathQuery.resolveAndProduct(
                singleQueryContext.getIndexReader(),
                singleQueryContext.getEmbeddedRoot(),
                operator.paths(),
                operator.query()))
        .mapOptionalChecked(buildSingle, BooleanClause.Occur.SHOULD);
  }

  private Optional<Query> singlePhraseQuery(
      StringPathQuery pq, Optional<FieldPath> embeddedRoot, int slop) throws InvalidQueryException {
    SafeQueryBuilder queryBuilder =
        this.queryFactoryContext.safeQueryBuilder(pq.getPath(), embeddedRoot);
    return validateFieldIndexedWithPositionInfoForPhraseQuery(
        pq,
        embeddedRoot,
        queryBuilder.createPhraseQuery(
            getLuceneFieldNameForStringPath(pq.getPath(), embeddedRoot), pq.getQuery(), slop));
  }

  private Optional<Query> singleSynonymPhraseQuery(
      StringPathQuery pq, Optional<FieldPath> embeddedRoot, int slop, String synonymMappingName)
      throws InvalidQueryException {
    SafeQueryBuilder synonymQueryBuilder =
        this.queryFactoryContext.synonymQueryBuilder(
            pq.getPath(), embeddedRoot, synonymMappingName);

    Optional<Query> synonymQuery =
        synonymQueryBuilder.createPhraseQuery(
            getLuceneFieldNameForStringPath(pq.getPath(), embeddedRoot), pq.getQuery(), slop);

    if (synonymQuery.isPresent()) {
      SafeQueryBuilder originalQueryBuilder =
          this.queryFactoryContext.safeQueryBuilder(pq.getPath(), embeddedRoot);
      Optional<Query> originalPhraseQuery =
          originalQueryBuilder.createPhraseQuery(
              getLuceneFieldNameForStringPath(pq.getPath(), embeddedRoot), pq.getQuery(), slop);

      // query over original term should always be present if synonyms query is present
      return validateFieldIndexedWithPositionInfoForPhraseQuery(
          pq,
          embeddedRoot,
          Optional.of(
              // boost "exact match" (docs that would've matched without synonyms) above synonym
              // matches. See exact details of boost in
              // SynonymQueryUtil::boostExactMatchSynonymQuery
              SynonymQueryUtil.boostExactMatchSynonymQuery(
                  synonymQuery.get(),
                  Check.isPresent(originalPhraseQuery, "originalPhraseQuery"),
                  SYNONYMS_EXACT_MATCH_ADDITIONAL_BOOST_VALUE)));
    }
    return Optional.empty();
  }

  /**
   * If the underlying query outputted by {@link org.apache.lucene.util.QueryBuilder} contains a
   * {@link org.apache.lucene.search.PhraseQuery}, ensure that the queried path is indexed with
   * position information
   */
  private Optional<Query> validateFieldIndexedWithPositionInfoForPhraseQuery(
      StringPathQuery pq, Optional<FieldPath> embeddedRoot, Optional<Query> query)
      throws InvalidQueryException {
    if (SafeQueryBuilder.containsPhraseQuery(query)) {
      this.queryFactoryContext
          .getQueryTimeMappingChecks()
          .validateStringFieldIsIndexedWithPositionInfo(
              pq.getPath(),
              embeddedRoot,
              String.format(
                  "field %s not indexed as string with indexOptions=offset or "
                      + "indexOptions=positions, which is required for phrase operator "
                      + "queries",
                  pq.getPath()));
    }
    return query;
  }
}
