package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.DatePoint;
import java.util.Optional;
import org.bson.BsonValue;

public record DateValue(DatePoint point) implements NonNullValue {

  public DateRangeBound getDateRangeBound() {
    // returns a range bound of [point, point] used by EqualsQueryFactory
    return new DateRangeBound(Optional.of(this.point), Optional.of(this.point), true, true);
  }

  @Override
  public ValueType getType() {
    return ValueType.DATE;
  }

  @Override
  public NonNullValueType getNonNullType() {
    return NonNullValueType.DATE;
  }

  @Override
  public BsonValue toBson() {
    return this.point.toBson();
  }
}
