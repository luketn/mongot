package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.value.NonNullValue;
import com.xgen.mongot.index.query.operators.value.ObjectIdValue;
import com.xgen.mongot.index.query.operators.value.StringValue;
import java.util.Arrays;
import java.util.List;
import org.bson.types.ObjectId;

interface ListInequalityBuilder<T extends ListInequalityBuilder<T>> {

  T values(List<NonNullValue> values);

  default T values(NonNullValue... values) {
    return values(Arrays.asList(values));
  }

  T addValue(NonNullValue value);

  default T addValue(long value) {
    return addValue(ValueBuilder.longNumber(value));
  }

  default T addValue(double value) {
    return addValue(ValueBuilder.doubleNumber(value));
  }

  default T addValue(String value) {
    return addValue(new StringValue(value));
  }

  default T addValue(ObjectId value) {
    return addValue(new ObjectIdValue(value));
  }
}
