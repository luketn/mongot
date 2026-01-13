package com.xgen.mongot.index.query.sort;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public enum SortOrder implements Encodable {
  ASC(1),
  DESC(-1);

  public final int intValue;

  SortOrder(int intValue) {
    this.intValue = intValue;
  }

  /** Returns true if the order is the reverse of the natural ordering. */
  public boolean isReverse() {
    return this == DESC;
  }

  /**
   * Returns the opposite SortOrder (i.e. ASC -> DESC, DESC -> ASC).
   */
  public SortOrder invert() {
    return this == ASC ? DESC : ASC;
  }

  public static SortOrder fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    int intValue;
    switch (value.getBsonType()) {
      case INT32:
      case INT64:
      case DOUBLE:
        intValue = value.asNumber().intValue();
        break;
      default:
        return context.handleUnexpectedType("number", value.getBsonType());
    }

    if (intValue == 1) {
      return ASC;
    }

    if (intValue == -1) {
      return DESC;
    }

    // TODO(CLOUDP-280897): should we allow any pos/neg number?
    return context.handleSemanticError("sort order must be 1 or -1");
  }

  @Override
  public BsonValue toBson() {
    return new BsonDouble(this.intValue);
  }
}
