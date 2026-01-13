package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.GaussianDecayExpression;
import com.xgen.mongot.index.query.scores.expressions.PathExpression;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class GaussianDecayExpressionBuilder {

  private Optional<PathExpression> path;
  private Optional<Double> origin;
  private Optional<Double> scale;
  private Optional<Double> offset = Optional.empty();
  private Optional<Double> decay = Optional.empty();

  public static GaussianDecayExpressionBuilder builder() {
    return new GaussianDecayExpressionBuilder();
  }

  public GaussianDecayExpressionBuilder path(PathExpression path) {
    this.path = Optional.of(path);
    return this;
  }

  public GaussianDecayExpressionBuilder origin(Double origin) {
    this.origin = Optional.of(origin);
    return this;
  }

  public GaussianDecayExpressionBuilder scale(Double scale) {
    this.scale = Optional.of(scale);
    return this;
  }

  public GaussianDecayExpressionBuilder offset(Double offset) {
    this.offset = Optional.of(offset);
    return this;
  }

  public GaussianDecayExpressionBuilder decay(Double decay) {
    this.decay = Optional.of(decay);
    return this;
  }

  public GaussianDecayExpression build() {
    Check.isPresent(this.path, "path");
    Check.isPresent(this.origin, "origin");
    Check.isPresent(this.scale, "scale");

    return new GaussianDecayExpression(
        this.path.get(),
        this.origin.get(),
        this.scale.get(),
        this.offset.orElse(GaussianDecayExpression.Fields.OFFSET.getDefaultValue()),
        this.decay.orElse(GaussianDecayExpression.Fields.DECAY.getDefaultValue()));
  }
}
