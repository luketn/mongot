package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.NumericPoint;
import java.util.Optional;
import org.bson.BsonValue;

public record NumericValue(NumericPoint point) implements NonNullValue {

  public NumericRangeBound getNumericRangeBound() {
    // returns a range bound of [point, point] used by EqualsQueryFactory
    return new NumericRangeBound(Optional.of(this.point), Optional.of(this.point), true, true);
  }

  @Override
  public ValueType getType() {
    return ValueType.NUMBER;
  }

  @Override
  public NonNullValueType getNonNullType() {
    return NonNullValueType.NUMBER;
  }

  @Override
  public BsonValue toBson() {
    return this.point.toBson();
  }
}
