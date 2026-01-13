package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.MultiTermQueryConstantScoreBlendedWrapperSpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

public class MultiTermQueryConstantScoreWrapperBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<List<QueryExplainInformation>> queries = Optional.empty();

  public static MultiTermQueryConstantScoreWrapperBuilder builder() {
    return new MultiTermQueryConstantScoreWrapperBuilder();
  }

  public MultiTermQueryConstantScoreWrapperBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public MultiTermQueryConstantScoreWrapperBuilder queries(List<QueryExplainInformation> queries) {
    this.queries = Optional.of(queries);
    return this;
  }

  public MultiTermQueryConstantScoreBlendedWrapperSpec build() {
    Check.isPresent(this.queries, "queries");

    return new MultiTermQueryConstantScoreBlendedWrapperSpec(this.path, this.queries.get());
  }
}
