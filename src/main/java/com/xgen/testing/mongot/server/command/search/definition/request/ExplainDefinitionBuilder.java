package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.server.command.search.definition.request.ExplainDefinition;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ExplainDefinitionBuilder {
  private Optional<Explain.Verbosity> verbosity;

  public static ExplainDefinitionBuilder builder() {
    return new ExplainDefinitionBuilder();
  }

  public ExplainDefinitionBuilder verbosity(Explain.Verbosity verbosityOptions) {
    this.verbosity = Optional.of(verbosityOptions);
    return this;
  }

  /** Builds the ExplainDefinition. */
  public ExplainDefinition build() {
    Check.isPresent(this.verbosity, "verbosity");

    return new ExplainDefinition(this.verbosity.get());
  }
}
