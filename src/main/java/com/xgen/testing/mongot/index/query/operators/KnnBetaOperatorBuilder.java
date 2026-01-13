package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;
import org.bson.BsonNumber;

public class KnnBetaOperatorBuilder
    extends PathOperatorBuilder<KnnBetaOperator, KnnBetaOperatorBuilder> {

  private Optional<List<BsonNumber>> vector = Optional.empty();
  private Optional<Operator> filter = Optional.empty();
  private Optional<Integer> k = Optional.empty();

  @Override
  protected KnnBetaOperatorBuilder getBuilder() {
    return this;
  }

  public KnnBetaOperatorBuilder vector(List<BsonNumber> vector) {
    this.vector = Optional.of(vector);
    return this;
  }

  public KnnBetaOperatorBuilder filter(Operator filter) {
    this.filter = Optional.of(filter);
    return this;
  }

  public KnnBetaOperatorBuilder k(int k) {
    this.k = Optional.of(k);
    return this;
  }

  @Override
  public KnnBetaOperator build() {
    Check.isPresent(this.vector, "vector");
    Check.isPresent(this.k, "k");

    return new KnnBetaOperator(
        getScore(), getPaths(), this.vector.get(), this.filter, this.k.get());
  }
}
