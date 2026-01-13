package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.PathExpression;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class PathExpressionBuilder {

  private Optional<FieldPath> value;
  private Optional<Double> undefined = Optional.empty();

  public static PathExpressionBuilder builder() {
    return new PathExpressionBuilder();
  }

  public PathExpressionBuilder value(String value) {
    this.value = Optional.of(FieldPath.parse(value));
    return this;
  }

  public PathExpressionBuilder undefined(Double undefined) {
    this.undefined = Optional.of(undefined);
    return this;
  }

  public PathExpression build() {
    Check.isPresent(this.value, "value");
    return new PathExpression(this.value.get(), this.undefined.orElse(0d));
  }
}
