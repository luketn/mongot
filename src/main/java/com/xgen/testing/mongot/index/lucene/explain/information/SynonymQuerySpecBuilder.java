package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.SynonymQuerySpec;
import com.xgen.mongot.util.FieldPath;
import java.util.List;
import java.util.Optional;

public class SynonymQuerySpecBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private List<String> values = List.of();

  public static SynonymQuerySpecBuilder builder() {
    return new SynonymQuerySpecBuilder();
  }

  public SynonymQuerySpecBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public SynonymQuerySpecBuilder values(List<String> values) {
    this.values = values;
    return this;
  }

  /** Builds a SynonymQuerySpec from a SynonymQuerySpecCreator. */
  public SynonymQuerySpec build() {
    return new SynonymQuerySpec(this.path, this.values);
  }
}
