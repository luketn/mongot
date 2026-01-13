package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.value.BooleanValue;
import com.xgen.mongot.index.query.operators.value.DateValue;
import com.xgen.mongot.index.query.operators.value.NumericValue;
import com.xgen.mongot.index.query.operators.value.ObjectIdValue;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import org.bson.types.ObjectId;

public class ValueBuilder {

  public static StringValue string(String str) {
    return new StringValue(str);
  }

  public static NumericValue intNumber(int intNum) {
    return new NumericValue(new LongPoint((long) intNum));
  }

  public static NumericValue longNumber(long longNum) {
    return new NumericValue(new LongPoint(longNum));
  }

  public static NumericValue doubleNumber(double doubleNum) {
    return new NumericValue(new DoublePoint(doubleNum));
  }

  public static BooleanValue bool(boolean bool) {
    return new BooleanValue(bool);
  }

  public static DateValue date(DatePoint datePoint) {
    return new DateValue(datePoint);
  }

  public static ObjectIdValue objectId(ObjectId objectId) {
    return new ObjectIdValue(objectId);
  }
}
