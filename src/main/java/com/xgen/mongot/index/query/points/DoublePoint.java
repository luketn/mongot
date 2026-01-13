package com.xgen.mongot.index.query.points;

import static com.xgen.mongot.util.NumericUtils.compareDoubleDouble;
import static com.xgen.mongot.util.NumericUtils.compareDoubleLong;

import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public record DoublePoint(double value) implements NumericPoint {

  /** Deserializes a Point from BSON. */
  public static DoublePoint fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return switch (value.getBsonType()) {
      case DOUBLE -> new DoublePoint(value.asDouble().getValue());
      default -> context.handleUnexpectedType("double", value.getBsonType());
    };
  }

  @Override
  public long getDoubleValueRepresentation() {
    return LuceneDoubleConversionUtils.toLong(this.value);
  }

  @Override
  public long getLongValueRepresentation() {
    return (long) this.value;
  }

  @Override
  public BsonValue toBson() {
    return new BsonDouble(this.value);
  }

  @Override
  public int compareTo(NumericPoint o) {
    return switch (o) {
      case LongPoint olp -> compareDoubleLong(this.value, olp.value());
      case DoublePoint odp -> compareDoubleDouble(this.value, odp.value());
    };
  }

  @Override
  public String toString() {
    return Double.toString(this.value);
  }
}
