package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.AutocompleteOperator;
import com.xgen.mongot.index.query.operators.FuzzyOption;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AutocompleteOperatorBuilder
    extends OperatorBuilder<AutocompleteOperator, AutocompleteOperatorBuilder> {

  private final List<String> query = new ArrayList<>();
  private Optional<FieldPath> path = Optional.empty();
  private Optional<FuzzyOption> fuzzy = Optional.empty();
  private Optional<AutocompleteOperator.TokenOrder> tokenOrder = Optional.empty();

  @Override
  protected AutocompleteOperatorBuilder getBuilder() {
    return this;
  }

  public AutocompleteOperatorBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public AutocompleteOperatorBuilder query(String query) {
    this.query.add(query);
    return this;
  }

  public AutocompleteOperatorBuilder fuzzy(FuzzyOption fuzzy) {
    this.fuzzy = Optional.of(fuzzy);
    return this;
  }

  public AutocompleteOperatorBuilder tokenOrder(AutocompleteOperator.TokenOrder tokenOrder) {
    this.tokenOrder = Optional.of(tokenOrder);
    return this;
  }

  @Override
  public AutocompleteOperator build() {
    Check.isPresent(this.path, "path");
    return new AutocompleteOperator(
        getScore(),
        this.path.get(),
        this.query,
        this.fuzzy,
        this.tokenOrder.orElse(AutocompleteOperator.Fields.TOKEN_ORDER.getDefaultValue()));
  }

  public static FuzzyOptionBuilder fuzzyBuilder() {
    return new FuzzyOptionBuilder();
  }
}
