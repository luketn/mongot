package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.ComparisonOperator;
import com.xgen.mongot.index.query.operators.value.ObjectIdValue;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.types.ObjectId;

public interface InequalityBuilder<T extends ComparisonOperator>
    extends MqlFilterOperatorBuilder<T> {

  InequalityBuilder<T> value(Value value);

  default InequalityBuilder<T> value(long value) throws BsonParseException {
    return value(ValueBuilder.longNumber(value));
  }

  default InequalityBuilder<T> value(double value) throws BsonParseException {
    return value(ValueBuilder.doubleNumber(value));
  }

  default InequalityBuilder<T> value(String value) throws BsonParseException {
    return value(new StringValue(value));
  }

  default InequalityBuilder<T> value(ObjectId value) throws BsonParseException {
    return value(new ObjectIdValue(value));
  }
}
