package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.EqualsOperator;
import com.xgen.mongot.index.query.operators.value.BooleanValue;
import com.xgen.mongot.index.query.operators.value.DateValue;
import com.xgen.mongot.index.query.operators.value.NullValue;
import com.xgen.mongot.index.query.operators.value.NumericValue;
import com.xgen.mongot.index.query.operators.value.ObjectIdValue;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.index.query.operators.value.UuidValue;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.FieldPath;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;

public class EqualsOperatorBuilder extends OperatorBuilder<EqualsOperator, EqualsOperatorBuilder> {

  private Optional<FieldPath> path = Optional.empty();
  private Optional<Value> value = Optional.empty();
  private Optional<List<String>> doesNotAffect = Optional.empty();

  public EqualsOperatorBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public EqualsOperatorBuilder doesNotAffect(List<String> doesNotAffect) {
    this.doesNotAffect = Optional.of(doesNotAffect);
    return this;
  }

  public EqualsOperatorBuilder doesNotAffect(String doesNotAffect) {
    this.doesNotAffect = Optional.of(List.of(doesNotAffect));
    return this;
  }

  public EqualsOperatorBuilder value(boolean value) {
    this.value = Optional.of(new BooleanValue(value));
    return this;
  }

  public EqualsOperatorBuilder value(ObjectId value) {
    this.value = Optional.of(new ObjectIdValue(value));
    return this;
  }

  public EqualsOperatorBuilder value(Date value) {
    this.value = Optional.of(new DateValue(new DatePoint(value)));
    return this;
  }

  public EqualsOperatorBuilder value(long value) {
    this.value = Optional.of(new NumericValue(new LongPoint(value)));
    return this;
  }

  public EqualsOperatorBuilder value(double value) {
    this.value = Optional.of(new NumericValue(new DoublePoint(value)));
    return this;
  }

  public EqualsOperatorBuilder value(String value) {
    this.value = Optional.of(new StringValue(value));
    return this;
  }

  public EqualsOperatorBuilder uuidValue(String uuidValue) {
    this.value = Optional.of(new UuidValue(UUID.fromString(uuidValue)));
    return this;
  }

  public EqualsOperatorBuilder nullValue() {
    this.value = Optional.of(new NullValue());
    return this;
  }

  @Override
  protected EqualsOperatorBuilder getBuilder() {
    return this;
  }

  @Override
  public EqualsOperator build() {
    return new EqualsOperator(getScore(), this.path.get(), this.value.get(), this.doesNotAffect);
  }
}
