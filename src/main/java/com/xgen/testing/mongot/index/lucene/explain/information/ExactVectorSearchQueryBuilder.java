package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.ExactVectorSearchQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.apache.lucene.index.VectorSimilarityFunction;

public class ExactVectorSearchQueryBuilder {

  private Optional<String> path = Optional.empty();
  private Optional<VectorSimilarityFunction> similarityFunction = Optional.empty();
  private Optional<QueryExplainInformation> filter = Optional.empty();

  public static ExactVectorSearchQueryBuilder builder() {
    return new ExactVectorSearchQueryBuilder();
  }

  public ExactVectorSearchQueryBuilder path(String path) {
    this.path = Optional.of(path);
    return this;
  }

  public ExactVectorSearchQueryBuilder similarityFunction(
      VectorSimilarityFunction similarityFunction) {
    this.similarityFunction = Optional.of(similarityFunction);
    return this;
  }

  public ExactVectorSearchQueryBuilder filter(QueryExplainInformation filter) {
    this.filter = Optional.of(filter);
    return this;
  }

  public ExactVectorSearchQuerySpec build() {
    Check.isPresent(this.path, "path");
    Check.isPresent(this.similarityFunction, "similarityFunction");

    return new ExactVectorSearchQuerySpec(
        this.path.get(), this.similarityFunction.get(), this.filter);
  }
}
