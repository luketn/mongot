package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimingBreakdown;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class QueryExplainInformationBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<LuceneQuerySpecification.Type> type = Optional.empty();
  private Optional<String> analyzer = Optional.empty();
  private Optional<LuceneQuerySpecification> args = Optional.empty();
  private Optional<ExplainTimingBreakdown> stats = Optional.empty();

  public static QueryExplainInformationBuilder builder() {
    return new QueryExplainInformationBuilder();
  }

  public QueryExplainInformationBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public QueryExplainInformationBuilder type(LuceneQuerySpecification.Type type) {
    this.type = Optional.of(type);
    return this;
  }

  public QueryExplainInformationBuilder analyzer(String analyzer) {
    this.analyzer = Optional.of(analyzer);
    return this;
  }

  public QueryExplainInformationBuilder args(LuceneQuerySpecification args) {
    this.args = Optional.of(args);
    return this;
  }

  public QueryExplainInformationBuilder stats(ExplainTimingBreakdown stats) {
    this.stats = Optional.of(stats);
    return this;
  }

  /** Builds QueryExplainInformation from an QueryExplainInformationCreator. */
  public QueryExplainInformation build() {
    Check.isPresent(this.args, "args");
    Check.isPresent(this.type, "type");

    return new QueryExplainInformation(
        this.path, this.type.get(), this.analyzer, this.args.get(), this.stats);
  }
}
