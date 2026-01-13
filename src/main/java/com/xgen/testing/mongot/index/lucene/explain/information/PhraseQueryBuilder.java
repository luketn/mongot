package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.PhraseQuerySpec;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class PhraseQueryBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private Optional<String> query = Optional.empty();
  private Optional<Integer> slop = Optional.empty();

  public static PhraseQueryBuilder builder() {
    return new PhraseQueryBuilder();
  }

  public PhraseQueryBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public PhraseQueryBuilder query(String query) {
    this.query = Optional.of(query);
    return this;
  }

  public PhraseQueryBuilder slop(int slop) {
    this.slop = Optional.of(slop);
    return this;
  }

  /** Builds a PhraseQuery from a PhraseQueryBuilder. */
  public PhraseQuerySpec build() {
    Check.isPresent(this.path, "path");
    Check.isPresent(this.query, "query");
    Check.isPresent(this.slop, "slop");

    return new PhraseQuerySpec(this.path.get(), this.query.get(), this.slop.get());
  }
}
