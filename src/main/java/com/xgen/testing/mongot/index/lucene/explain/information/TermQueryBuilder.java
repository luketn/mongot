package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.TermQuerySpec;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class TermQueryBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<String> value = Optional.empty();

  public static TermQueryBuilder builder() {
    return new TermQueryBuilder();
  }

  public TermQueryBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public TermQueryBuilder value(String value) {
    this.value = Optional.of(value);
    return this;
  }

  /** Builds the TermQuery from the TermQueryBuilder. */
  public TermQuerySpec build() {
    Check.isPresent(this.path, "path");
    Check.isPresent(this.value, "value");

    return new TermQuerySpec(this.path.get(), this.value.get());
  }
}
