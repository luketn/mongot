package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.ReturnScope;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.util.CollectionUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.Query;

class GenericDrillSidewaysFactory {

  static Map<String, LuceneDrillSideways> from(
      Map<String, Query> facetToOperatorQueries,
      FacetCollector collector,
      LuceneIndexSearcher searcher,
      LuceneFacetContext facetContext,
      Optional<ReturnScope> returnScope)
      throws IOException, InvalidQueryException {

    MongotDrillSideways genericDrillSideways =
        getDrillSideways(collector, searcher, facetContext, returnScope.map(ReturnScope::path));
    return CollectionUtils.newMapFromKeys(
        facetToOperatorQueries,
        (facetName, operatorQuery) ->
            new LuceneDrillSideways(
                genericDrillSideways, getLuceneDrillDownQuery(facetName, operatorQuery)));
  }

  private static DrillDownQuery getLuceneDrillDownQuery(
      String facetName, Query facetOperatorQuery) {
    DrillDownQuery drillDownQuery =
        new DrillDownQuery(LuceneFacetContext.FACETS_CONFIG, facetOperatorQuery);
    drillDownQuery.add(facetName);
    return drillDownQuery;
  }

  private static MongotDrillSideways getDrillSideways(
      FacetCollector collector,
      LuceneIndexSearcher searcher,
      LuceneFacetContext facetContext,
      Optional<FieldPath> returnScope) {

    return new MongotDrillSideways(
        searcher,
        new FacetsConfig(),
        collector,
        facetContext,
        returnScope,
        searcher.getFacetsState(),
        searcher.getTokenFacetsStateCache(),
        Optional.empty());
  }
}
