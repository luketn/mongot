package com.xgen.testing.mongot.index.query.operators.mql;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.query.operators.mql.MqlFilterOperator;
import com.xgen.mongot.index.query.operators.mql.SimpleClause;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SimpleClauseBuilder {
  private Optional<FieldPath> path = Optional.empty();
  private List<MqlFilterOperator> operators = new ArrayList<>();

  public SimpleClauseBuilder path(String path) {
    return path(FieldPath.parse(path));
  }

  public SimpleClauseBuilder path(FieldPath path) {
    this.path = Optional.of(path);
    return this;
  }

  public SimpleClauseBuilder operators(List<MqlFilterOperator> operators) {
    this.operators = operators;
    return this;
  }

  public SimpleClauseBuilder addOperator(MqlFilterOperator operator) {
    this.operators.add(operator);
    return this;
  }

  public SimpleClauseBuilder addOperator(MqlFilterOperatorBuilder<?> operator)
      throws BsonParseException {
    this.operators.add(operator.build());
    return this;
  }

  public SimpleClause build() {
    Check.isPresent(this.path, "path");
    checkArg(!this.operators.isEmpty(), "must have at least 1 operator");
    return new SimpleClause(this.path.get(), this.operators);
  }
}
