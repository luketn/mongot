package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

/**
 * Generates a query matches all documents.
 *
 * <p>Note that if the index uses embedded fields, one MongoDB document may be represented with
 * multiple Lucene documents. In that case, we actually want this query to only match the one "root"
 * Lucene document, and will apply a filter to enforce this.
 */
class AllDocsQueryFactory {

  private final SearchQueryFactoryContext context;

  AllDocsQueryFactory(SearchQueryFactoryContext context) {
    this.context = context;
  }

  Query fromAllDocuments(SingleQueryContext singleQueryContext) {
    if (this.context.isIndexWithEmbeddedFields()) {
      return EmbeddedDocumentQueryFactory.parentFilter(singleQueryContext.getEmbeddedRoot());
    }

    return new MatchAllDocsQuery();
  }
}
