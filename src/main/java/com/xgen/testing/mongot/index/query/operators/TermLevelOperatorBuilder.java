package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.query.operators.TermLevelOperator;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class TermLevelOperatorBuilder<
        T extends TermLevelOperator, B extends TermLevelOperatorBuilder<T, B>>
    extends QueryOperatorBuilder<T, B> {

  private Optional<Boolean> allowAnalyzedField = Optional.empty();
  private final List<UnresolvedStringPath> path = new ArrayList<>();

  public B path(String path) {
    this.path.add(UnresolvedStringPathBuilder.fieldPath(path));
    return getBuilder();
  }

  public B path(UnresolvedStringPath path) {
    this.path.add(path);
    return getBuilder();
  }

  public B allowAnalyzedField(boolean allowAnalyzedField) {
    this.allowAnalyzedField = Optional.of(allowAnalyzedField);
    return getBuilder();
  }

  Boolean getAllowAnalyzed() {
    return this.allowAnalyzedField.orElse(
        TermLevelOperator.Fields.ALLOW_ANALYZED_FIELD.getDefaultValue());
  }

  public List<UnresolvedStringPath> getPath() {
    return this.path;
  }
}
