package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import com.xgen.mongot.index.version.IndexFormatVersion;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/** This class is used for $vectorSearch queries running against vector index type. */
public class LuceneVectorQueryFactoryDistributor {

  private final VectorSearchQueryFactory vectorSearchQueryFactory;
  private final VectorFieldDefinitionResolver definitionResolver;

  private LuceneVectorQueryFactoryDistributor(
      VectorSearchQueryFactory vectorSearchQueryFactory,
      VectorFieldDefinitionResolver definitionResolver) {
    this.vectorSearchQueryFactory = vectorSearchQueryFactory;
    this.definitionResolver = definitionResolver;
  }

  public static LuceneVectorQueryFactoryDistributor create(
      VectorIndexDefinition indexDefinition, IndexFormatVersion ifv, FeatureFlags featureFlags) {
    var definitionResolver = new VectorFieldDefinitionResolver(indexDefinition, ifv);
    var factoryContext = new VectorQueryFactoryContext(definitionResolver);
    var equalsQueryFactory = new EqualsQueryFactory(factoryContext);
    var existsQueryFactory = new ExistsQueryFactory(factoryContext);
    var rangeQueryFactory =
        new RangeQueryFactory(
            factoryContext, equalsQueryFactory, indexDefinition.getIndexCapabilities(ifv));
    var inQueryFactory = new InQueryFactory(factoryContext, featureFlags);
    return new LuceneVectorQueryFactoryDistributor(
        new VectorSearchQueryFactory(
            factoryContext,
            new VectorSearchFilterQueryFactory(
                factoryContext,
                rangeQueryFactory,
                inQueryFactory,
                existsQueryFactory,
                equalsQueryFactory)),
        definitionResolver);
  }

  public Query createQuery(MaterializedVectorSearchQuery query, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    return this.vectorSearchQueryFactory.fromQuery(
        query.materializedCriteria(), SingleQueryContext.createQueryRoot(indexReader));
  }

  /**
   * Creates a WrappedKnnQuery from an operator to be used for explain queries.
   *
   * @param materializedVectorQuery vector search query
   * @param indexReader index reader
   * @return Lucene Query
   * @throws InvalidQueryException represents a parsing exception
   */
  public Query createExplainQuery(
      MaterializedVectorSearchQuery materializedVectorQuery, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    var singleQueryContext = SingleQueryContext.createExplainRoot(indexReader);
    return new WrappedKnnQuery(
        this.vectorSearchQueryFactory.fromQuery(
            materializedVectorQuery.materializedCriteria(), singleQueryContext));
  }

  public VectorFieldDefinitionResolver getDefinitionResolver() {
    return this.definitionResolver;
  }
}
