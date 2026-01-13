package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class LtOperator extends OpenRangeBoundComparisonOperator {

  private final Point value;

  public LtOperator(Point value, RangeBound<? extends Comparable<?>> bounds) {
    super(bounds);
    this.value = value;
  }

  public Point getValue() {
    return this.value;
  }

  @Override
  public Category getCategory() {
    return Category.UPPER_BOUND;
  }

  @Override
  public BsonValue operatorToBson() {
    return this.value.toBson();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.LT, Optional.of(this)).build();
  }

  public static LtOperator fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    var value = Values.VALUE.getParser().parse(context, bsonValue);
    var bounds = convertToRangeBounds(context, Optional.empty(), Optional.of(value), false, false);
    return new LtOperator(value, bounds);
  }
}
