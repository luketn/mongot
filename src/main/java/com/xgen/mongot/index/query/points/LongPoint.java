package com.xgen.mongot.index.query.points;

import static com.xgen.mongot.util.NumericUtils.compareLongDouble;
import static com.xgen.mongot.util.NumericUtils.compareLongLong;

import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public record LongPoint(long value) implements NumericPoint {

  /** Deserializes a NumericPoint from BSON. */
  public static LongPoint fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return switch (value.getBsonType()) {
      case INT32 -> new LongPoint(value.asInt32().getValue());
      case INT64 -> new LongPoint(value.asInt64().getValue());
      default -> context.handleUnexpectedType("int32, int64", value.getBsonType());
    };
  }

  @Override
  public long getDoubleValueRepresentation() {
    return LuceneDoubleConversionUtils.toLong(this.value);
  }

  @Override
  public long getLongValueRepresentation() {
    return this.value;
  }

  @Override
  public BsonValue toBson() {
    return new BsonInt64(this.value);
  }

  @Override
  public int compareTo(NumericPoint o) {
    return switch (o) {
      case LongPoint olp -> compareLongLong(this.value, olp.value());
      case DoublePoint odp -> compareLongDouble(this.value, odp.value());
    };
  }

  @Override
  public String toString() {
    return Long.toString(this.value);
  }
}
